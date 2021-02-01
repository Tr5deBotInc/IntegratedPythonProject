from FrameworkBaseClasses.ProcessBaseClass import ProcessBaseClass
from assets import constants as Constant

import ccxt
import time
from datetime import datetime
import mysql.connector


class IndicatorGenerationBaseClass(ProcessBaseClass):
    def __init__(self):
        # print("Indicator Generation Base Class Constructor")
        self.ProcessName = "Indicator Generation Process"
        super().__init__()

    # region Functions used to retrieve information from the exchange

    # This function will retrieve the latest (LimitInt) candles
    def get1mCandles(self, SinceInt: int):
        # print("get 1m Candles")
        if self.ExchangeConnectionObj.has['fetchOHLCV']:
            time.sleep(self.ExchangeConnectionObj.rateLimit / 1000)
            try:
                CandlestickDataArr = self.ExchangeConnectionObj.fetch_ohlcv(
                    self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                    "1m",
                    since=SinceInt
                )
                return CandlestickDataArr

            except ccxt.NetworkError as ErrorMessage:
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.now(),
                    "fetch_ohlcv("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + "," + "1m,since="
                    + str(SinceInt) + ")",
                    "NetworkError: " + str(ErrorMessage)
                )
            except ccxt.ExchangeError as ErrorMessage:
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.now(),
                    "fetch_ohlcv("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + "," + "1m,since="
                    + str(SinceInt) + ")",
                    "ExchangeError: " + str(ErrorMessage)
                )
            except Exception as ErrorMessage:
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.now(),
                    "fetch_ohlcv("
                    + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    + "," + "1m,since="
                    + str(SinceInt) + ")",
                    "OtherError: " + str(ErrorMessage)
                )

        else:
            return False
    # endregion