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
    CustomVariables = {}

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
                        if float(BinanceAssetObj['netAsset']) > 0.0002:
                            return ProjectFunctions.truncateFloat(abs(float(BinanceAssetObj['netAsset'])), 4)
                        elif float(BinanceAssetObj['netAsset']) < -0.0002:
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
        OrderQuantityInt = format(abs(self.CurrentSystemVariables['CurrentAccountPositionSize']), '.8f')
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
                self.CurrentSystemVariables['CurrentPortfolioValue'],
                self.CurrentSystemVariables['CurrentAccountPositionSize'],
                self.CurrentSystemVariables['TradingState']
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
                self.CurrentSystemVariables['CurrentPortfolioValue'],
                self.CurrentSystemVariables['CurrentAccountPositionSize'],
                self.CurrentSystemVariables['TradingState']
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
                self.CurrentSystemVariables['CurrentPortfolioValue'],
                self.CurrentSystemVariables['CurrentAccountPositionSize'],
                self.CurrentSystemVariables['TradingState']
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

    def placeMarketOrder(self, OrderSideStr, QuantityInt=None, BorrowBool=False):
        if QuantityInt is None:
            QuantityInt = abs(self.CurrentSystemVariables['CurrentAccountPositionSize'])
        try:
            if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
                if BorrowBool:
                    self.ExchangeConnectionObj.sapi_post_margin_order({
                        'symbol': 'BTCUSDT',
                        'side': OrderSideStr.upper(),
                        'type': 'MARKET',
                        'quantity': QuantityInt,
                        'sideEffectType': 'MARGIN_BUY',
                        'timestamp': str(round(time.time() * 1000))
                    })
                else:
                    self.ExchangeConnectionObj.sapi_post_margin_order({
                        'symbol': 'BTCUSDT',
                        'side': OrderSideStr.upper(),
                        'type': 'MARKET',
                        'quantity': QuantityInt,
                        'timestamp': str(round(time.time() * 1000))
                    })
            else:
                self.ExchangeConnectionObj.create_order(
                    self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
                    'market',
                    OrderSideStr,
                    QuantityInt,
                    {'type': 'market'}
                )
            self.createOrderLog(
                datetime.now(),
                self.CurrentSystemVariables['CurrentPrice'],
                'market',
                OrderSideStr,
                QuantityInt,
                self.CurrentSystemVariables['CurrentPortfolioValue'],
                self.CurrentSystemVariables['CurrentAccountPositionSize'],
                self.CurrentSystemVariables['TradingState']
            )

            return True
        except ccxt.NetworkError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order("
                + self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                + ", 'market'," +
                OrderSideStr + "," + str(QuantityInt) + ", " +
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
                OrderSideStr + "," + str(QuantityInt) + ", " +
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
                OrderSideStr + "," + str(QuantityInt) + ", " +
                "market" + ")",
                "OtherError: " + str(ErrorMessage)
            )

        return False

    def getOrderQuantity(self):
        AlgorithmExposureFloat = float(self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EXPOSURE_INDEX])
        if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
            return float(self.CurrentSystemVariables['CurrentPortfolioValue']) * AlgorithmExposureFloat / \
                   self.CurrentSystemVariables['CurrentPrice']
        elif self.ExchangeConnectionDetails['ExchangeName'] == Constant.BITMEX_EXCHANGE_ID:
            CurrentPositionObj = self.ExchangeConnectionObj.private_get_position()
            return CurrentPositionObj[0]['currentQty']

    def getCurrentPrice(self):
        TradingPairSymbolStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
        try:
            self.CurrentSystemVariables['CurrentPrice'] = self.ExchangeConnectionObj.fetch_ticker(TradingPairSymbolStr)['bid']
            self.createPriceLogEntry(datetime.now(), self.CurrentSystemVariables['CurrentPrice'])
        except Exception as ErrorMessage:
            # Please create a log table and a log function for exchange related retrievals.
            # We will only log errors in this table
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "WebSocket get_ticket()['mid]", ErrorMessage
            )

    def checkTradingState(self):
        if self.CurrentSystemVariables['TradingState'] == 'Market Halt':
            if self.CurrentSystemVariables['CurrentAccountPositionSize'] == 0 and self.OpenOrderCountInt > 0:
                self.cancelAllOrders()
                self.createProcessExecutionLog(self.ProcessName, datetime.now(),
                                               "Process Update: Closing all orders on Market Halt trading state")
                return False
            elif self.OpenOrderCountInt == 0 and self.CurrentSystemVariables['CurrentAccountPositionSize'] == 0:
                return False
        elif self.CurrentSystemVariables['TradingState'] == 'Market Dead Stop' or \
                self.CurrentSystemVariables['TradingState'] == 'Manual Halt':
            if self.OpenOrderCountInt > 0:
                self.cancelAllOrders()
            if self.CurrentSystemVariables['CurrentAccountPositionSize'] != 0:
                if self.CurrentSystemVariables['CurrentAccountPositionSize'] > 0:
                    self.placeMarketOrder('sell')
                elif self.CurrentSystemVariables['CurrentAccountPositionSize'] < 0:
                    self.placeMarketOrder('buy')

                self.createProcessExecutionLog(self.ProcessName, datetime.now(),
                                               "Process Update: Creating market orders on open position due to "
                                               + self.CurrentSystemVariables['TradingState'] + " trading state")
                if self.OpenOrderCountInt > 0:
                    self.cancelAllOrders()
                    self.createProcessExecutionLog(self.ProcessName, datetime.now(),
                                                   "Process Update: Closing all orders due to "
                                                   + self.CurrentSystemVariables['TradingState'] + " trading state")
            return False
        elif self.CurrentSystemVariables['TradingState'] is None:
            # In case the algorithm configuration variables are not set yet, we do not execute trading functionality
            self.createProcessExecutionLog(self.ProcessName, datetime.now(),
                                           "Process Update: Algorithm trading state not set")
            return False

        return True
