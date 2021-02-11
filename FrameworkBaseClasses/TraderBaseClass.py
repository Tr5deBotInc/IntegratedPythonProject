from FrameworkBaseClasses.ProcessBaseClass import ProcessBaseClass
from assets import ProjectFunctions
from assets import constants as Constant

import math
import ccxt
from datetime import datetime
import mysql.connector
import time


class TraderBaseClass(ProcessBaseClass):
    OpenOrderCountInt = 0
    OpenPositionCountInt = 0
    CurrentOrderArr = []
    MarginTradingCurrency = 'BTC'

    def __init__(self):
        # print("Trader Base Class Constructor")
        self.ProcessName = "Trader Process"
        self.ObjectTypeValidationArr = {
            'Indicators': ['BB', 'RSI', 'SMA', 'EMA_RETEST'],
            'SystemVariables': [
                'CurrentPrice',
                'CurrentAccountBalance',
                'CurrentPortfolioValue',
                'SystemState',
                'TradingState'
            ],
            'DatabaseDetails': ['ServerName', 'DatabaseName', 'UserName', 'Password'],
            'ExchangeDetails': ['ExchangeName', 'ApiKey', 'ApiSecret']
        }

        super().__init__()

    def countOpenOrders(self):
        try:
            self.CurrentOrderArr = self.ExchangeConnectionObj.fetch_open_orders(
                self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
            )
            return len(self.CurrentOrderArr)
        except ccxt.NetworkError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "fetch_open_orders("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ")", "NetworkError: " + str(ErrorMessage)
            )
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "fetch_open_orders("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ")", "ExchangeError: " + str(ErrorMessage)
            )
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "fetch_open_orders("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ")", "OtherError: " + str(ErrorMessage)
            )
        return False

    def checkPosition(self):
        try:
            if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                BinanceAssetObjArr = self.ExchangeConnectionObj.fetchBalance()['info']['userAssets']
                for BinanceAssetObj in BinanceAssetObjArr:
                    if BinanceAssetObj['asset'] == self.MarginTradingCurrency:
                        if float(BinanceAssetObj['netAsset']) > 0.0001:
                            return ProjectFunctions.truncateFloat(abs(float(BinanceAssetObj['netAsset'])), 4)
                        elif float(BinanceAssetObj['netAsset']) < -0.0001:
                            return ProjectFunctions.truncateFloat(-abs(float(BinanceAssetObj['netAsset'])), 4)
            else:
                CurrentPositionObj = self.ExchangeConnectionObj.private_get_position()
                return CurrentPositionObj[0]['currentQty']
        except ccxt.NetworkError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "private_get_position()",
                "NetworkError: " + str(ErrorMessage)
            )
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "private_get_position()",
                "ExchangeError: " + str(ErrorMessage)
            )
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "private_get_position()",
                "OtherError: " + str(ErrorMessage)
            )
        return False

    def cancelAllOrders(self):
        CancelFailedBool = False
        for CurrentOrder in self.CurrentOrderArr:
            try:
                if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                    self.ExchangeConnectionObj.cancel_order(CurrentOrder['id'], CurrentOrder['symbol'])
                else:
                    self.ExchangeConnectionObj.cancel_order(CurrentOrder['id'])
            except ccxt.NetworkError as ErrorMessage:
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.now(),
                    "cancel_order(" + CurrentOrder['id'] + ")",
                    "NetworkError: " + str(ErrorMessage)
                )
                CancelFailedBool = True
            except ccxt.ExchangeError as ErrorMessage:
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.now(),
                    "cancel_order(" + CurrentOrder['id'] + ")",
                    "ExchangeError: " + str(ErrorMessage)
                )
                CancelFailedBool = True
            except Exception as ErrorMessage:
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.now(),
                    "cancel_order(" + CurrentOrder['id'] + ")",
                    "OtherError: " + str(ErrorMessage)
                )
                CancelFailedBool = True
        return not CancelFailedBool

    def placeClosingOrder(self, OrderSideStr):
        OrderParameterObj = {}
        OrderQuantityInt = format(abs(self.OpenPositionCountInt), '.8f')
        try:
            if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                self.ExchangeConnectionObj.sapi_post_margin_order({
                    'symbol': 'BTCUSDT',
                    'side': OrderSideStr.upper(),
                    'type': 'LIMIT',
                    'quantity': OrderQuantityInt,
                    'price': round(self.IndicatorsObj['SMA']['value']),
                    'sideEffectType': 'MARGIN_BUY',
                    'timeInForce': 'GTC',
                    'timestamp': str(round(time.time() * 1000))
                })
            else:
                self.ExchangeConnectionObj.create_order(
                    self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                    'limit',
                    OrderSideStr,
                    OrderQuantityInt,
                    round(self.IndicatorsObj['SMA']['value']),
                    OrderParameterObj
                )
            self.createOrderLog(
                datetime.now(),
                round(self.IndicatorsObj['SMA']['value']),
                'close',
                OrderSideStr,
                OrderQuantityInt,
                self.CurrentSystemVariables['CurrentPortfolioValue']
            )
        except ccxt.NetworkError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'limit'," +
                OrderSideStr + "," + str(OrderQuantityInt) + ", " +
                str(round(self.IndicatorsObj['SMA']['value'])) + ")",
                "NetworkError: " + str(ErrorMessage)
            )
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'limit'," +
                OrderSideStr + "," + str(OrderQuantityInt) + ", " +
                str(round(self.IndicatorsObj['SMA']['value'])) + ")",
                "ExchangeError: " + str(ErrorMessage)
            )
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'limit'," +
                OrderSideStr + "," + str(OrderQuantityInt) + ", " +
                str(round(self.IndicatorsObj['SMA']['value'])) + ")",
                "OtherError: " + str(ErrorMessage)
            )

    def placeOpeningOrders(self):
        UpperLimitArr = [self.IndicatorsObj['BB']['upper'], self.IndicatorsObj['RSI']['upper']]
        LowerLimitArr = [self.IndicatorsObj['BB']['lower'], self.IndicatorsObj['RSI']['lower']]
        OrderQuantityInt = format(self.getOrderQuantity(), '.4f')
        try:
            OrderSideStr = 'sell'
            if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                self.ExchangeConnectionObj.sapi_post_margin_order({
                    'symbol': 'BTCUSDT',
                    'side': OrderSideStr.upper(),
                    'type': 'LIMIT',
                    'quantity': OrderQuantityInt,
                    'price': round(max(UpperLimitArr)),
                    'sideEffectType': 'MARGIN_BUY',
                    'timeInForce': 'GTC',
                    'timestamp': str(round(time.time() * 1000))
                })
            else:
                self.ExchangeConnectionObj.create_order(
                    self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                    'limit', OrderSideStr,
                    OrderQuantityInt,
                    round(max(UpperLimitArr))
                )
            self.createOrderLog(
                datetime.now(),
                round(max(UpperLimitArr)),
                'open',
                OrderSideStr,
                OrderQuantityInt,
                self.CurrentSystemVariables['CurrentPortfolioValue']
            )
        except ccxt.NetworkError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'limit', 'sell', " + str(OrderQuantityInt) + "," +
                str(round(max(UpperLimitArr))) + ")",
                "NetworkError: " + str(ErrorMessage)
            )
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'limit', 'sell', " + str(OrderQuantityInt) + "," +
                str(round(max(UpperLimitArr))) + ")",
                "ExchangeError: " + str(ErrorMessage)
            )
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'limit', 'sell', " + str(OrderQuantityInt) + "," +
                str(round(max(UpperLimitArr))) + ")",
                "OtherError: " + str(ErrorMessage)
            )

        try:
            OrderSideStr = 'buy'
            if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                self.ExchangeConnectionObj.sapi_post_margin_order({
                    'symbol': 'BTCUSDT',
                    'side': OrderSideStr.upper(),
                    'type': 'LIMIT',
                    'quantity': OrderQuantityInt,
                    'price': round(min(LowerLimitArr)),
                    'sideEffectType': 'MARGIN_BUY',
                    'timeInForce': 'GTC',
                    'timestamp': str(round(time.time() * 1000))
                })
            else:
                self.ExchangeConnectionObj.create_order(
                    self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                    'limit', OrderSideStr,
                    OrderQuantityInt,
                    round(min(LowerLimitArr))
                )
            self.createOrderLog(
                datetime.now(),
                round(min(LowerLimitArr)),
                'open',
                OrderSideStr,
                OrderQuantityInt,
                self.CurrentSystemVariables['CurrentPortfolioValue']
            )
        except ccxt.NetworkError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'limit', 'buy', " + str(OrderQuantityInt) + "," +
                str(round(min(LowerLimitArr))) + ")",
                "NetworkError: " + str(ErrorMessage)
            )
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'limit', 'buy', " + str(OrderQuantityInt) + "," +
                str(round(min(LowerLimitArr))) + ")",
                "ExchangeError: " + str(ErrorMessage)
            )
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'limit', 'buy', " + str(OrderQuantityInt) + "," +
                str(round(min(LowerLimitArr))) + ")",
                "OtherError: " + str(ErrorMessage)
            )

    def placeMarketOrder(self, OrderSideStr):
        try:
            if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                self.ExchangeConnectionObj.sapi_post_margin_order({
                    'symbol': 'BTCUSDT',
                    'side': OrderSideStr.upper(),
                    'type': 'MARKET',
                    'quantity': -self.OpenPositionCountInt,
                    'timestamp': str(round(time.time() * 1000))
                })
            else:
                self.ExchangeConnectionObj.create_order(
                    self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                    'market',
                    OrderSideStr,
                    -self.OpenPositionCountInt,
                    {'type': 'market'}
                )
            self.createOrderLog(
                datetime.now(),
                'market',
                'close',
                OrderSideStr,
                -self.OpenPositionCountInt,
                self.CurrentSystemVariables['CurrentPortfolioValue']
            )
        except ccxt.NetworkError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'market'," +
                OrderSideStr + "," + str(-self.OpenPositionCountInt) + ", " +
                "market" + ")",
                "NetworkError: " + str(ErrorMessage)
            )
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'market'," +
                OrderSideStr + "," + str(-self.OpenPositionCountInt) + ", " +
                "market" + ")",
                "ExchangeError: " + str(ErrorMessage)
            )
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'market'," +
                OrderSideStr + "," + str(-self.OpenPositionCountInt) + ", " +
                "market" + ")",
                "OtherError: " + str(ErrorMessage)
            )

    def getOrderQuantity(self):
        AlgorithmExposureFloat = float(self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EXPOSURE_INDEX])
        if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
            return float(self.CurrentSystemVariables['CurrentPortfolioValue']) * AlgorithmExposureFloat / \
                   self.CurrentSystemVariables['CurrentPrice']
        elif self.ExchangeConnectionDetails['ExchangeName'] == Constant.BITMEX_EXCHANGE_ID:
            CurrentPositionObj = self.ExchangeConnectionObj.private_get_position()
            return CurrentPositionObj[0]['currentQty']
