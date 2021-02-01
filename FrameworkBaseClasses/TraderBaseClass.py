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
            'Indicators': ['BB', 'RSI', 'SMA'],
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
            self.CurrentOrderArr = self.ExchangeConnectionObj.fetch_open_orders(Constant.CURRENCY_SYMBOL)
            return len(self.CurrentOrderArr)
        except ccxt.NetworkError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "fetch_open_orders(" + Constant.CURRENCY_SYMBOL + ")",
                "NetworkError: " + str(ErrorMessage)
            )
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "fetch_open_orders(" + Constant.CURRENCY_SYMBOL + ")",
                "ExchangeError: " + str(ErrorMessage)
            )
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "fetch_open_orders(" + Constant.CURRENCY_SYMBOL + ")",
                "OtherError: " + str(ErrorMessage)
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
                    Constant.CURRENCY_SYMBOL,
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
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'limit'," +
                OrderSideStr + "," + str(OrderQuantityInt) + ", " +
                str(round(self.IndicatorsObj['SMA']['value'])) + ")",
                "NetworkError: " + str(ErrorMessage)
            )
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'limit'," +
                OrderSideStr + "," + str(OrderQuantityInt) + ", " +
                str(round(self.IndicatorsObj['SMA']['value'])) + ")",
                "ExchangeError: " + str(ErrorMessage)
            )
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'limit'," +
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
                    Constant.CURRENCY_SYMBOL,
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
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'limit', 'sell', " + str(OrderQuantityInt) + "," +
                str(round(max(UpperLimitArr))) + ")",
                "NetworkError: " + str(ErrorMessage)
            )
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'limit', 'sell', " + str(OrderQuantityInt) + "," +
                str(round(max(UpperLimitArr))) + ")",
                "ExchangeError: " + str(ErrorMessage)
            )
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'limit', 'sell', " + str(OrderQuantityInt) + "," +
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
                    Constant.CURRENCY_SYMBOL,
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
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'limit', 'buy', " + str(OrderQuantityInt) + "," +
                str(round(min(LowerLimitArr))) + ")",
                "NetworkError: " + str(ErrorMessage)
            )
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'limit', 'buy', " + str(OrderQuantityInt) + "," +
                str(round(min(LowerLimitArr))) + ")",
                "ExchangeError: " + str(ErrorMessage)
            )
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'limit', 'buy', " + str(OrderQuantityInt) + "," +
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
                    Constant.CURRENCY_SYMBOL,
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
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'market'," +
                OrderSideStr + "," + str(-self.OpenPositionCountInt) + ", " +
                "market" + ")",
                "NetworkError: " + str(ErrorMessage)
            )
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'market'," +
                OrderSideStr + "," + str(-self.OpenPositionCountInt) + ", " +
                "market" + ")",
                "ExchangeError: " + str(ErrorMessage)
            )
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "create_order(" + Constant.CURRENCY_SYMBOL +
                ", 'market'," +
                OrderSideStr + "," + str(-self.OpenPositionCountInt) + ", " +
                "market" + ")",
                "OtherError: " + str(ErrorMessage)
            )

    def getOrderQuantity(self):
        if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
            return float(self.CurrentSystemVariables['CurrentPortfolioValue']) * Constant.ALGORITHM_EXPOSURE / \
                   self.CurrentSystemVariables['CurrentPrice']
        elif self.ExchangeConnectionDetails['ExchangeName'] == Constant.BITMEX_EXCHANGE_ID:
            CurrentPositionObj = self.ExchangeConnectionObj.private_get_position()
            return CurrentPositionObj[0]['currentQty']

    # region Functions used to long process successes and failures as system executes
    def templateDatabaseLogger(self, QueryStr, QueryData, FunctionNameStr=" "):
        try:
            ConnectionObj = mysql.connector.connect(host=self.DatabaseConnectionDetails['ServerName'],
                                                 database=self.DatabaseConnectionDetails['DatabaseName'],
                                                 user=self.DatabaseConnectionDetails['UserName'],
                                                 password=self.DatabaseConnectionDetails['Password'])

            CursorObj = ConnectionObj.cursor()
            CursorObj.execute(QueryStr, QueryData)
            ConnectionObj.commit()

            if ConnectionObj.is_connected():
                CursorObj.close()
                ConnectionObj.close()
            else:
                print("Failed to close MySQL connection")

        except mysql.connector.Error as error:
            print(self.ProcessName + " in " + FunctionNameStr + " failed to insert into MySQL table {}".format(error))
            print(QueryStr)
            print(QueryData)
            print(datetime.now())

    def createProcessExecutionLog(self, ProcessNameStr, EntryDateTimeObj, MessageStr):
        # print("create Process Execution Log")
        QueryStr = """INSERT INTO ProcessExecutionLog (ProcessName, EntryTime, Message) 
                               VALUES 
                               (%s, %s, %s)"""

        QueryData = (
            ProcessNameStr,
            EntryDateTimeObj,
            MessageStr
        )

        self.templateDatabaseLogger(QueryStr, QueryData, "createProcessExecutionLog")

    def createExchangeInteractionLog(self, ProcessNameStr, EntryDateTimeObj, ExchangeFunctionStr, MessageStr):
        QueryStr = """INSERT INTO ExchangeInteractionFailureLog (ProcessName, EntryTime, ExchangeFunction, ErrorMessage)
                                       VALUES
                                       (%s, %s, %s, %s)"""

        QueryData = (
            ProcessNameStr,
            EntryDateTimeObj,
            ExchangeFunctionStr,
            MessageStr
        )
        self.templateDatabaseLogger(QueryStr, QueryData, "createExchangeInteractionLog")

    def createIndicatorUpdateLog(self, ProcessNameStr, EntryDateTimeObj, IndicatorNameStr, IndicatorDataObj, SuccessStr):
        QueryStr = """INSERT INTO IndicatorGenerationLog (EntryTime, IndicatorData, Success, ProcessName, IndicatorName)
                                       VALUES
                                       (%s, %s, %s, %s, %s)"""

        QueryData = (
            EntryDateTimeObj,
            str(IndicatorDataObj),
            SuccessStr,
            ProcessNameStr,
            IndicatorNameStr
        )
        self.templateDatabaseLogger(QueryStr, QueryData, "createIndicatorUpdateLog")

    def createOrderLog(self, EntryDateTimeObj, OrderPriceFloat, OrderActionStr, OrderDirectionStr, OrderQuantityInt, PortfolioValueFloat):
        QueryStr = """INSERT INTO OrderLog (EntryTime, OrderPrice, OrderAction, OrderDirection, OrderQuantity, PortfolioValue)
                                               VALUES
                                               (%s, %s, %s, %s, %s, %s)"""

        QueryData = (
            EntryDateTimeObj,
            OrderPriceFloat,
            OrderActionStr,
            OrderDirectionStr,
            str(OrderQuantityInt),
            PortfolioValueFloat
        )
        self.templateDatabaseLogger(QueryStr, QueryData, "createOrderLog")

    # endregion
