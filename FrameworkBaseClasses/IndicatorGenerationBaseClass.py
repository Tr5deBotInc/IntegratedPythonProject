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
                    Constant.CURRENCY_SYMBOL,
                    "1m",
                    since=SinceInt
                )
                return CandlestickDataArr

            except ccxt.NetworkError as ErrorMessage:
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.now(),
                    "fetch_ohlcv(" + Constant.CURRENCY_SYMBOL + "," + "1m,since="
                    + str(SinceInt) + ")",
                    "NetworkError: " + str(ErrorMessage)
                )
            except ccxt.ExchangeError as ErrorMessage:
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.now(),
                    "fetch_ohlcv(" + Constant.CURRENCY_SYMBOL + "," + "1m,since="
                    + str(SinceInt) + ")",
                    "ExchangeError: " + str(ErrorMessage)
                )
            except Exception as ErrorMessage:
                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.now(),
                    "fetch_ohlcv(" + Constant.CURRENCY_SYMBOL + "," + "1m,since="
                    + str(SinceInt) + ")",
                    "OtherError: " + str(ErrorMessage)
                )

        else:
            return False
    # endregion

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

    # endregion
