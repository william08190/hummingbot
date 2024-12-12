import base64
import hmac
import json
from typing import Any, Dict, List
from urllib.parse import urlencode

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class BitgetPerpetualAuth(AuthBase):
    """
    Bitget Perpetual API 的认证类
    """
    def __init__(self, api_key: str, secret_key: str, passphrase: str, time_provider: TimeSynchronizer):
        self._api_key: str = api_key
        self._secret_key: str = secret_key
        self._passphrase: str = passphrase
        self._time_provider: TimeSynchronizer = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        headers = {
            "Content-Type": "application/json",
            "ACCESS-KEY": self._api_key,
            "ACCESS-TIMESTAMP": str(int(self._time_provider.time() * 1e3)),
            "ACCESS-PASSPHRASE": self._passphrase,
        }

        path = request.throttler_limit_id
        query_string = urlencode(request.params) if request.params else ""
        if query_string:
            path += f"?{query_string}"

        body = json.dumps(request.data) if request.data else ""
        pre_hash_string = self._pre_hash(headers["ACCESS-TIMESTAMP"], request.method.value, path, body)
        headers["ACCESS-SIGN"] = self._sign(pre_hash_string, self._secret_key)

        request.headers.update(headers)
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        配置 WebSocket 请求以进行身份验证
        """
        return request  # 直接返回

    def get_ws_auth_payload(self) -> List[Dict[str, Any]]:
        """
        生成用于 WebSocket 认证的负载
        :return: 包含认证信息的字典列表
        """
        timestamp = str(int(self._time_provider.time() * 1e3))
        pre_hash_string = self._pre_hash(timestamp, "GET", "/user/verify", "")
        signature = self._sign(pre_hash_string, self._secret_key)
        auth_info = [{
            "apiKey": self._api_key,
            "passphrase": self._passphrase,
            "timestamp": timestamp,
            "sign": signature,
        }]
        return auth_info

    @staticmethod
    def _sign(message: str, secret_key: str) -> str:
        mac = hmac.new(secret_key.encode('utf-8'), message.encode('utf-8'), digestmod='sha256')
        return base64.b64encode(mac.digest()).decode().strip()

    @staticmethod
    def _pre_hash(timestamp: str, method: str, request_path: str, body: str) -> str:
        return f"{timestamp}{method.upper()}{request_path}{body}"
