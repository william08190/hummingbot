import asyncio
import sys
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.derivative.bitget_perpetual import (
    bitget_perpetual_constants as CONSTANTS,
    bitget_perpetual_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest, WSPlainTextRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.bitget_perpetual.bitget_perpetual_derivative import BitgetPerpetualDerivative


class BitgetPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):

    FULL_ORDER_BOOK_RESET_DELTA_SECONDS = sys.maxsize

    def __init__(
        self,
        trading_pairs: List[str],
        connector: 'BitgetPerpetualDerivative',
        api_factory: WebAssistantsFactory,
        domain: str = ""
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._diff_messages_queue_key = "books"
        self._trade_messages_queue_key = "trade"
        self._funding_info_messages_queue_key = "current-fundRate"
        self._pong_response_event = None

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        funding_info_response = await self._request_complete_funding_info(trading_pair)
        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(funding_info_response["indexPrice"]),
            mark_price=Decimal(funding_info_response["markPrice"]),
            next_funding_utc_timestamp=int(int(funding_info_response["nextFundingTime"]) * 1e-3),
            rate=Decimal(funding_info_response["fundingRate"]),
        )
        return funding_info

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        while True:
            try:
                await asyncio.wait_for(
                    super()._process_websocket_messages(websocket_assistant=websocket_assistant),
                    timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE
                )
            except asyncio.TimeoutError:
                if self._pong_response_event and not self._pong_response_event.is_set():
                    raise IOError("The user stream channel is unresponsive (pong response not received)")
                self._pong_response_event = asyncio.Event()
                await self._send_ping(websocket_assistant=websocket_assistant)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if event_message == CONSTANTS.WS_PONG_RESPONSE and self._pong_response_event:
            self._pong_response_event.set()
        elif "event" in event_message:
            if event_message["event"] == "error":
                raise IOError(f"Public channel subscription failed ({event_message})")
        elif "arg" in event_message:
            channel = event_message["arg"].get("channel")
            if channel == CONSTANTS.WS_ORDER_BOOK_EVENTS_TOPIC and event_message.get("action") == "snapshot":
                channel = self._snapshot_messages_queue_key

        return channel

    async def _request_complete_funding_info(self, trading_pair: str) -> Dict[str, Any]:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
        params = {"symbol": symbol}

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.get_rest_url_for_endpoint(CONSTANTS.GET_LAST_FUNDING_RATE_PATH_URL),
            throttler_limit_id=CONSTANTS.GET_LAST_FUNDING_RATE_PATH_URL,
            params=params,
            method=RESTMethod.GET,
        )
        return data["data"]

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=CONSTANTS.WSS_URL, message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE
        )
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            payloads = []
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair_without_product_type(trading_pair)
                for channel in [
                    self._diff_messages_queue_key,
                    self._trade_messages_queue_key,
                    self._funding_info_messages_queue_key,
                ]:
                    payloads.append({
                        "instType": "mc",
                        "channel": channel,
                        "instId": symbol,
                    })
            final_payload = {
                "op": "subscribe",
                "args": payloads,
            }
            subscribe_request = WSJSONRequest(payload=final_payload)
            await ws.send(subscribe_request)
            self.logger().info("Subscribed to public order book, trade and funding info channels...")
        except Exception as e:
            self.logger().exception(f"Error subscribing to channels: {str(e)}")
            raise

    async def _send_ping(self, websocket_assistant: WSAssistant):
        ping_request = WSPlainTextRequest(payload=CONSTANTS.WS_PING_REQUEST)
        await websocket_assistant.send(ping_request)
