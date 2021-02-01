from FrameworkBaseClasses.ProcessBaseClass import ProcessBaseClass
from assets import environments as EnvironmentDetails
from assets import constants as Constant
import sys as SystemObj

import ccxt
import mysql.connector
import threading
import traceback
import time
from datetime import datetime


class ManagerBaseClass:
    ProcessName = 'Manager Process'
    ThreadInstantiationArr = []
    DatabaseConnectionDetails = {
        'ServerName': '',
        'DatabaseName': '',
        'UserName': '',
        'Password': ''
    }
    ExchangeConnectionDetails = {
        'ExchangeName': '',
        'ApiKey': '',
        'ApiSecret': ''
    }
    ExchangeConnectionObj = None
    SystemVariablesObj = {
        'AlgorithmId': 'bb_rsi_2020',
        'CurrentPrice': None,
        'CurrentAccountBalance': None,
        'CurrentPortfolioValue': None,
        'SystemState': None,
        'TradingState': None
    }

    BaseCurrency = 'USDT'

    def __init__(self):
        # print("Manager Base Class Constructor")
        self.requestProjectInitiationState()
        self.initializeSystemData()

    def startProcessThreading(self):
        # print("start Process Threading")
        for ProcessInfoObj in self.ThreadInstantiationArr:
            threading.Thread(target=self.executeEachTimeInterval,
                             args=(ProcessInfoObj['ProcessObj'], ProcessInfoObj['IntervalInt'])).start()

    def executeEachTimeInterval(self, ProcessObj: ProcessBaseClass, IntervalInt):
        # print("execute Each Time Interval")
        StartingTimeInt = time.time()
        while True:  # change this to work based on system state
            self.createProcessExecutionLog(ProcessObj.ProcessName, datetime.now(), "Starting Process Execution")

            try:
                ProcessObj.initiateExecution()
                self.createProcessExecutionLog(ProcessObj.ProcessName, datetime.now(),
                                               "Process Executed Successfully")
            except Exception as ErrorMessage:
                self.createProcessExecutionLog(ProcessObj.ProcessName, datetime.now(),
                                               "Process Failed: " + str(ErrorMessage) + "\n" + traceback.format_exc())

            if time.time() - StartingTimeInt < IntervalInt:
                time.sleep(IntervalInt - (time.time() - StartingTimeInt))
                StartingTimeInt = time.time()
            else:
                self.createProcessExecutionLog(
                    ProcessObj.ProcessName,
                    datetime.now(),
                    "Process took " + str(time.time() - StartingTimeInt) + " seconds to execute")
                StartingTimeInt = time.time()

    def requestProjectInitiationState(self):
        # print("request Project Initiation State")
        SystemStateInputStr = input("Please provide the system state:\n1. Active\n2. Testing\n3. Backtesting\nInput: ")
        if SystemStateInputStr.strip() == '1':
            self.setSystemState("Active")
        elif SystemStateInputStr.strip() == '2':
            self.setSystemState("Testing")
        elif SystemStateInputStr.strip() == '3':
            self.setSystemState("Backtesting")
        else:
            print("Invalid selection")
            SystemObj.exit()

    def setSystemState(self, SystemStateStr):
        # print("set System State")
        self.SystemVariablesObj['SystemState'] = SystemStateStr
        if SystemStateStr == 'Active':
            self.DatabaseConnectionDetails['ServerName'] = EnvironmentDetails.HOST
            self.DatabaseConnectionDetails['DatabaseName'] = EnvironmentDetails.DATABASE
            self.DatabaseConnectionDetails['UserName'] = EnvironmentDetails.USER
            self.DatabaseConnectionDetails['Password'] = EnvironmentDetails.PASSWORD

            self.setExchangeConnection()

        elif SystemStateStr == 'Testing':
            pass

        elif SystemStateStr == 'Backtesting':
            pass

    def setExchangeConnection(self):
        # print("set Exchange Connection")
        ExchangeInputStr = input("Please select an exchange:\n1. Bitmex\n2. Binance\nInput: ")
        if ExchangeInputStr.strip() == '1':
            self.ExchangeConnectionDetails['ExchangeName'] = Constant.BITMEX_EXCHANGE_ID
            self.ExchangeConnectionDetails['ApiKey'] = EnvironmentDetails.BITMEX_API_KEY
            self.ExchangeConnectionDetails['ApiSecret'] = EnvironmentDetails.BITMEX_API_SECRET
            try:
                ExchangeClassObj = getattr(ccxt, self.ExchangeConnectionDetails['ExchangeName'])
                self.ExchangeConnectionObj = ExchangeClassObj({
                    'apiKey': self.ExchangeConnectionDetails['ApiKey'],
                    'secret': self.ExchangeConnectionDetails['ApiSecret'],
                    'timeout': 30000,
                    'enableRateLimit': True,
                    'symbols': [Constant.CURRENCY_SYMBOL]
                })

            except Exception as ErrorMessage:
                print("Something went wrong when setting up Exchange connection: " + str(ErrorMessage))
        elif ExchangeInputStr.strip() == '2':
            self.ExchangeConnectionDetails['ExchangeName'] = Constant.BINANCE_EXCHANGE_ID
            self.ExchangeConnectionDetails['ApiKey'] = EnvironmentDetails.BINANCE_API_KEY
            self.ExchangeConnectionDetails['ApiSecret'] = EnvironmentDetails.BINANCE_API_SECRET

            try:
                ExchangeClassObj = getattr(ccxt, self.ExchangeConnectionDetails['ExchangeName'])
                self.ExchangeConnectionObj = ExchangeClassObj({
                    'apiKey': self.ExchangeConnectionDetails['ApiKey'],
                    'secret': self.ExchangeConnectionDetails['ApiSecret'],
                    'timeout': 30000,
                    'enableRateLimit': True,
                    'symbols': [Constant.CURRENCY_SYMBOL],
                    'options': {
                        'defaultType': 'margin'
                    }
                })
            except Exception as ErrorMessage:
                print("Something went wrong when setting up Exchange connection for Binance: " + str(ErrorMessage))
                SystemObj.exit()
        else:
            print("Invalid selection")
            SystemObj.exit()

        self.ExchangeConnectionObj.load_markets()

    # region Functions that need to be overwritten in the child class
    # This function is to be overwritten by the child class
    def initializeSystemData(self):
        pass

    # endregion

    # region Functions used to retrieve information from the exchange
    # This function will retrieve the latest (LimitInt) candles
    def get1mCandles(self, LimitInt: int):
        # print("get 1m Candles")
        if self.ExchangeConnectionObj.has['fetchOHLCV']:
            time.sleep(self.ExchangeConnectionObj.rateLimit / 1000)
            try:
                CandlestickDataArr = self.ExchangeConnectionObj.fetch_ohlcv(
                    Constant.CURRENCY_SYMBOL,
                    "1m",
                    since=round((time.time()*1000) - (1000 * LimitInt * 60)),
                    limit=LimitInt
                )
                return CandlestickDataArr
            except Exception as ErrorMessage:

                self.createExchangeInteractionLog(
                    self.ProcessName,
                    datetime.now(),
                    "fetch_ohlcv(" + Constant.CURRENCY_SYMBOL + "," + "1m,since=("
                    + str((time.time()*1000) - (1000 * LimitInt * 60)) + "),limit=" + str(LimitInt) + ")",
                    ErrorMessage
                )

        else:
            return False

    def getCurrentPrice(self):
        try:
            self.SystemVariablesObj['CurrentPrice'] = self.ExchangeConnectionObj.fetch_ticker(Constant.CURRENCY_SYMBOL)['bid']
        except Exception as ErrorMessage:
            # Please create a log table and a log function for exchange related retrievals.
            # We will only log errors in this table
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                " self.ExchangeConnectionObj.fetch_ticker('BTC/USDT')['bid']", ErrorMessage
            )

    def getCurrentBalance(self):
        BalanceObj = None
        try:
            BalanceObj = self.ExchangeConnectionObj.fetch_balance()
        except ccxt.NetworkError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "fetch_balance()",
                "NetworkError: " + str(ErrorMessage)
            )
            return
        except ccxt.ExchangeError as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "fetch_balance()",
                "ExchangeError: " + str(ErrorMessage)
            )
            return
        except Exception as ErrorMessage:
            self.createExchangeInteractionLog(
                self.ProcessName,
                datetime.now(),
                "fetch_balance()",
                "OtherError: " + str(ErrorMessage)
            )
            return
        if self.ExchangeConnectionDetails['ExchangeName'] == Constant.BINANCE_EXCHANGE_ID:
            self.SystemVariablesObj['CurrentPortfolioValue'] = float(BalanceObj['info']['totalNetAssetOfBtc']) * \
                                                               self.SystemVariablesObj['CurrentPrice']
            for AssetObj in BalanceObj['info']['userAssets']:
                if AssetObj['asset'] == self.BaseCurrency:
                    self.SystemVariablesObj['CurrentAccountBalance'] = AssetObj['free']
                    return
        elif self.ExchangeConnectionDetails['ExchangeName'] == Constant.BITMEX_EXCHANGE_ID:
            self.SystemVariablesObj['CurrentAccountBalance'] = BalanceObj['free']['BTC']
            self.SystemVariablesObj['CurrentPortfolioValue'] = BalanceObj['total']['BTC']
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
        print(ProcessNameStr)
        print(EntryDateTimeObj)
        print(ExchangeFunctionStr)
        print(MessageStr)
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

    def createPriceLogEntry(self, EntryDateTimeObj, CurrencyPrice):
        # print("create Process Execution Log")
        QueryStr = """INSERT INTO PriceLog (ExchangeName, CurrencySymbol, EntryTime, CurrencyPrice) 
                                      VALUES 
                                      (%s, %s, %s, %s)"""

        QueryData = (
            self.ExchangeConnectionDetails['ExchangeName'],
            Constant.CURRENCY_SYMBOL,
            EntryDateTimeObj,
            CurrencyPrice
        )

        self.templateDatabaseLogger(QueryStr, QueryData, "createPriceLogEntry")
    # endregion
