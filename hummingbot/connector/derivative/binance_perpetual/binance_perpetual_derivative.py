import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from bidict import bidict

import hummingbot.connector.derivative.bitget_perpetual.bitget_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.bitget_perpetual import (
    bitget_perpetual_utils,
    bitget_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.bitget_perpetual.bitget_perpetual_api_order_book_data_source import (
    BitgetPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.bitget_perpetual.bitget_perpetual_auth import BitgetPerpetualAuth
from hummingbot.connector.derivative.bitget_perpetual.bitget_perpetual_user_stream_data_source import (
    BitgetPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase, TradeFeeSchema
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_decimal_NaN = Decimal("nan")
s_decimal_0 = Decimal(0)


class BitgetPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils

    def __init__(
        self,
        client_config_map: "ClientConfigAdapter",
        bitget_perpetual_api_key: str = None,
        bitget_perpetual_secret_key: str = None,
        bitget_perpetual_passphrase: str = None,
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = "",
    ):
        self.bitget_perpetual_api_key = bitget_perpetual_api_key
        self.bitget_perpetual_secret_key = bitget_perpetual_secret_key
        self.bitget_perpetual_passphrase = bitget_perpetual_passphrase
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._last_trade_history_timestamp = None

        super().__init__(client_config_map)

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> BitgetPerpetualAuth:
        return BitgetPerpetualAuth(
            api_key=self.bitget_perpetual_api_key,
            secret_key=self.bitget_perpetual_secret_key,
            passphrase=self.bitget_perpetual_passphrase,
            time_provider=self._time_synchronizer)

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS
    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return None

    @property
    def client_order_id_prefix(self) -> str:
        return ""

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.QUERY_SYMBOL_ENDPOINT

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.QUERY_SYMBOL_ENDPOINT

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET]

    def supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY, PositionMode.HEDGE]

    async def check_network(self) -> NetworkStatus:
        result = NetworkStatus.NOT_CONNECTED
        try:
            response = await self._api_get(path_url=self.check_network_request_path, return_err=True)
            if response.get("flag", False):
                result = NetworkStatus.CONNECTED
        except asyncio.CancelledError:
            raise
        except Exception:
            result = NetworkStatus.NOT_CONNECTED
        return result
    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        position_action: PositionAction = PositionAction.NIL,
        **kwargs,
    ) -> Tuple[str, float]:
        data = {
            "side": f"{position_action.name.lower()}_{'long' if trade_type == TradeType.BUY else 'short'}",
            "symbol": await self.exchange_symbol_associated_to_pair(trading_pair),
            "marginCoin": self.get_buy_collateral_token(trading_pair),
            "size": str(amount),
            "orderType": "limit" if order_type.is_limit_type() else "market",
            "timeInForceValue": CONSTANTS.DEFAULT_TIME_IN_FORCE,
            "clientOid": order_id,
        }
        if order_type.is_limit_type():
            data["price"] = str(price)

        resp = await self._api_post(
            path_url=CONSTANTS.PLACE_ACTIVE_ORDER_PATH_URL,
            data=data,
            is_auth_required=True,
        )

        if resp["code"] != CONSTANTS.RET_CODE_OK:
            formatted_ret_code = self._format_ret_code_for_print(resp["code"])
            raise IOError(f"Error submitting order {order_id}: {formatted_ret_code} - {resp['msg']}")

        return str(resp["data"]["orderId"]), self.current_timestamp

    async def _update_balances(self):
        balances = {}
        trading_pairs_product_types = set([await self.product_type_for_trading_pair(trading_pair=trading_pair)
                                           for trading_pair in self.trading_pairs])
        product_types = trading_pairs_product_types or CONSTANTS.ALL_PRODUCT_TYPES

        for product_type in product_types:
            body_params = {"productType": product_type.lower()}
            wallet_balance: Dict[str, Any] = await self._api_get(
                path_url=CONSTANTS.GET_WALLET_BALANCE_PATH_URL,
                params=body_params,
                is_auth_required=True,
            )

            if wallet_balance["code"] != CONSTANTS.RET_CODE_OK:
                formatted_ret_code = self._format_ret_code_for_print(wallet_balance["code"])
                raise IOError(f"{formatted_ret_code} - {wallet_balance['msg']}")

            balances[product_type] = wallet_balance["data"]

        self._account_available_balances.clear()
        self._account_balances.clear()
        for product_type_balances in balances.values():
            for balance_data in product_type_balances:
                asset_name = balance_data["marginCoin"]
                queried_available = Decimal(str(balance_data["fixedMaxAvailable"]))
                self._account_available_balances[asset_name] = queried_available
                queried_total = Decimal(str(balance_data["equity"]))
                self._account_balances[asset_name] = queried_total
    async def _update_positions(self):
        """
        Retrieves all positions using the REST API.
        """
        position_data = []
        product_types = CONSTANTS.ALL_PRODUCT_TYPES

        for product_type in product_types:
            body_params = {"productType": product_type.lower()}
            raw_response: Dict[str, Any] = await self._api_get(
                path_url=CONSTANTS.GET_POSITIONS_PATH_URL,
                params=body_params,
                is_auth_required=True,
            )
            position_data.extend(raw_response["data"])

        for position in position_data:
            ex_trading_pair = position.get("symbol")
            hb_trading_pair = await self.trading_pair_associated_to_exchange_symbol(ex_trading_pair)
            position_side = PositionSide.LONG if position["holdSide"] == "long" else PositionSide.SHORT
            unrealized_pnl = Decimal(str(position["unrealizedPL"]))
            entry_price = Decimal(str(position["averageOpenPrice"]))
            amount = Decimal(str(position["total"]))
            leverage = Decimal(str(position["leverage"]))
            pos_key = self._perpetual_trading.position_key(hb_trading_pair, position_side)
            if amount != s_decimal_0:
                position_obj = Position(
                    trading_pair=hb_trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount * (Decimal("-1.0") if position_side == PositionSide.SHORT else Decimal("1.0")),
                    leverage=leverage,
                )
                self._perpetual_trading.set_position(pos_key, position_obj)
            else:
                self._perpetual_trading.remove_position(pos_key)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        if order.exchange_order_id is not None:
            try:
                all_fills_response = await self._request_order_fills(order=order)
                fills_data = all_fills_response.get("data", [])

                for fill_data in fills_data:
                    trade_update = self._parse_trade_update(trade_msg=fill_data, tracked_order=order)
                    trade_updates.append(trade_update)
            except IOError as ex:
                if not self._is_request_exception_related_to_time_synchronizer(request_exception=ex):
                    raise

        return trade_updates
    async def _user_stream_event_listener(self):
        """
        Listens to message in _user_stream_tracker.user_stream queue.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                endpoint = event_message["arg"]["channel"]
                payload = event_message["data"]

                if endpoint == CONSTANTS.WS_SUBSCRIPTION_POSITIONS_ENDPOINT_NAME:
                    await self._process_account_position_event(payload)
                elif endpoint == CONSTANTS.WS_SUBSCRIPTION_ORDERS_ENDPOINT_NAME:
                    for order_msg in payload:
                        self._process_trade_event_message(order_msg)
                        self._process_order_event_message(order_msg)
                        self._process_balance_update_from_order_event(order_msg)
                elif endpoint == CONSTANTS.WS_SUBSCRIPTION_WALLET_ENDPOINT_NAME:
                    for wallet_msg in payload:
                        self._process_wallet_event_message(wallet_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")

    async def _process_account_position_event(self, position_entries: List[Dict[str, Any]]):
        """
        Updates position snapshot received from WebSocket.
        """
        all_position_keys = []

        for position_msg in position_entries:
            ex_trading_pair = position_msg["instId"]
            trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=ex_trading_pair)
            position_side = PositionSide.LONG if position_msg["holdSide"] == "long" else PositionSide.SHORT
            entry_price = Decimal(str(position_msg["averageOpenPrice"]))
            amount = Decimal(str(position_msg["total"]))
            leverage = Decimal(str(position_msg["leverage"]))
            unrealized_pnl = Decimal(str(position_msg["upl"]))
            pos_key = self._perpetual_trading.position_key(trading_pair, position_side)
            all_position_keys.append(pos_key)
            if amount != s_decimal_0:
                position = Position(
                    trading_pair=trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount * (Decimal("-1.0") if position_side == PositionSide.SHORT else Decimal("1.0")),
                    leverage=leverage,
                )
                self._perpetual_trading.set_position(pos_key, position)
            else:
                self._perpetual_trading.remove_position(pos_key)

        # If positions are removed from snapshot, they should be cleared
        position_keys = list(self.account_positions.keys())
        positions_to_remove = (position_key for position_key in position_keys if position_key not in all_position_keys)
        for position_key in positions_to_remove:
            self._perpetual_trading.remove_position(position_key)
