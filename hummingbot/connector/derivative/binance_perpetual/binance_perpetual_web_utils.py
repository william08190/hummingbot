from typing import Callable, Optional

from hummingbot.connector.derivative.bitget_perpetual import bitget_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

def public_rest_url(path_url: str, domain: str = None) -> str:
    """
    构建公共 REST 接口的完整 URL。
    :param path_url: 公共 REST 接口路径。
    :param domain: Bitget 域名，默认为 "com"。
    :return: 完整的接口 URL。
    """
    return get_rest_url_for_endpoint(path_url, domain)

def private_rest_url(path_url: str, domain: str = None) -> str:
    """
    构建私有 REST 接口的完整 URL。
    :param path_url: 私有 REST 接口路径。
    :param domain: Bitget 域名，默认为 "com"。
    :return: 完整的接口 URL。
    """
    return get_rest_url_for_endpoint(path_url, domain)

def get_rest_url_for_endpoint(endpoint: str, domain: str = None) -> str:
    """
    根据端点和域名构建完整的 REST 接口 URL。
    :param endpoint: 接口端点路径。
    :param domain: Bitget 域名，默认为 "com"。
    :return: 完整的接口 URL。
    """
    base_url = CONSTANTS.REST_URL
    if domain:
        base_url = base_url.replace("com", domain)
    return f"{base_url}{endpoint}"

def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """
    构建 API 工厂，包含节流器、时间同步器和认证信息。
    :param throttler: 异步节流器实例。
    :param time_synchronizer: 时间同步器实例。
    :param time_provider: 时间提供者函数。
    :param auth: 认证信息实例。
    :return: 配置完成的 WebAssistantsFactory 实例。
    """
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time(throttler=throttler))
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ],
    )
    return api_factory

def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    """
    构建不包含时间同步预处理器的 API 工厂。
    :param throttler: 异步节流器实例。
    :return: 配置完成的 WebAssistantsFactory 实例。
    """
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory

def create_throttler() -> AsyncThrottler:
    """
    创建并配置异步节流器。
    :return: 配置完成的 AsyncThrottler 实例。
    """
    throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
    return throttler

async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None, domain: str = ""
) -> float:
    """
    获取当前服务器时间（毫秒）。
    :param throttler: 异步节流器实例。
    :param domain: Bitget 域名，默认为空字符串。
    :return: 服务器时间的时间戳（毫秒）。
    """
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    url = public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH_URL, domain=domain)
    response = await rest_assistant.execute_request(
        url=url,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH_URL,
        method=RESTMethod.GET,
        return_err=True,
    )
    server_time = float(response["data"]["serverTime"])

    return server_time
