from decimal import Decimal
from typing import Any, Dict

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

# 根据最新的Bitget费率信息更新默认费率
# 现货交易：挂单（Maker）和吃单（Taker）均为0.1%
# 合约交易：挂单（Maker）为0.02%，吃单（Taker）为0.06%
# 请注意，使用BGB支付手续费可享受20%的折扣
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.001"),  # 0.1%
    taker_percent_fee_decimal=Decimal("0.001"),  # 0.1%
)

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDT"

def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    验证交易对的交易信息是否有效
    :param exchange_info: 交易对的交易信息
    :return: 如果交易对有效，返回True；否则返回False
    """
    symbol = exchange_info.get("symbol")
    return symbol is not None and symbol.count("_") <= 1

class BitgetPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="bitget_perpetual", client_data=None)
    bitget_perpetual_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "请输入您的Bitget Perpetual API密钥",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    bitget_perpetual_secret_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "请输入您的Bitget Perpetual秘密密钥",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    bitget_perpetual_passphrase: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "请输入您的Bitget Perpetual口令",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "bitget_perpetual"

KEYS = BitgetPerpetualConfigMap.construct()
