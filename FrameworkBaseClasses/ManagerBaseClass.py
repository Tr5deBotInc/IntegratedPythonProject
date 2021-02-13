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


class ManagerBaseClass(ProcessBaseClass):
    ProcessName = 'Manager Process'
    ThreadInstantiationArr = []
    SystemVariablesObj = {
        'AlgorithmId': None,
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

    def requestProjectAlgorithmSelection(self):
        AlgorithmNameArr = self.getAlgorithmNames()
        AlgorithmOptionsStr = ""
        for AlgorithmNameIndexInt in range(0, len(AlgorithmNameArr)):
            AlgorithmOptionsStr += str(AlgorithmNameIndexInt+1) + ". " + AlgorithmNameArr[AlgorithmNameIndexInt][0] + '\n'
        SelectedAlgoeithmNameInputStr = input("Please select an algorithm:\n" + AlgorithmOptionsStr + "Input: ")

        if 0 <= int(SelectedAlgoeithmNameInputStr) <= len(AlgorithmNameArr):
            self.AlgorithmConfigurationObj = \
                self.getAlgorithmConfigurationObj(AlgorithmNameArr[int(SelectedAlgoeithmNameInputStr) - 1][0])
            self.SystemVariablesObj['AlgorithmId'] = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]
            self.SystemVariablesObj['TradingState'] = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_STATE_INDEX]
        self.setExchangeConnection()

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
            self.DatabaseConnectionDetails['ServerName'] = EnvironmentDetails.MULTIPLE_ALGORITHM_HOST
            self.DatabaseConnectionDetails['DatabaseName'] = EnvironmentDetails.MULTIPLE_ALGORITHM_DATABASE
            self.DatabaseConnectionDetails['UserName'] = EnvironmentDetails.MULTIPLE_ALGORITHM_USER
            self.DatabaseConnectionDetails['Password'] = EnvironmentDetails.MULTIPLE_ALGORITHM_PASSWORD

            self.requestProjectAlgorithmSelection()

        elif SystemStateStr == 'Testing':
            pass

        elif SystemStateStr == 'Backtesting':
            pass

    def setExchangeConnection(self):
        # print("set Exchange Connection")
        self.ExchangeConnectionDetails['ExchangeName'] = \
            self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EXCHANGE_NAME_INDEX]
        self.ExchangeConnectionDetails['ApiKey'] = \
            self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_API_KEY_INDEX]
        self.ExchangeConnectionDetails['ApiSecret'] = \
            self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_API_SECRET_INDEX]
        if self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EXCHANGE_NAME_INDEX] == \
                Constant.BITMEX_EXCHANGE_ID:
            try:
                ExchangeClassObj = getattr(ccxt, self.ExchangeConnectionDetails['ExchangeName'])
                self.ExchangeConnectionObj = ExchangeClassObj({
                    'apiKey': self.ExchangeConnectionDetails['ApiKey'],
                    'secret': self.ExchangeConnectionDetails['ApiSecret'],
                    'timeout': 30000,
                    'enableRateLimit': True,
                    'symbols': [
                        self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    ]
                })

            except Exception as ErrorMessage:
                print("Something went wrong when setting up Exchange connection: " + str(ErrorMessage))
        elif self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_EXCHANGE_NAME_INDEX] == \
                Constant.BINANCE_EXCHANGE_ID:
            try:
                ExchangeClassObj = getattr(ccxt, self.ExchangeConnectionDetails['ExchangeName'])
                self.ExchangeConnectionObj = ExchangeClassObj({
                    'apiKey': self.ExchangeConnectionDetails['ApiKey'],
                    'secret': self.ExchangeConnectionDetails['ApiSecret'],
                    'timeout': 30000,
                    'enableRateLimit': True,
                    'symbols': [
                        self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
                    ],
                    'options': {
                        'defaultType': 'margin'
                    }
                })
            except Exception as ErrorMessage:
                print("Something went wrong when setting up Exchange connection for Binance: " + str(ErrorMessage))
                SystemObj.exit()
        else:
            print("Invalid Exchange Name Selection")
            SystemObj.exit()

        self.ExchangeConnectionObj.load_markets()

    # region Functions that need to be overwritten in the child class
    # This function is to be overwritten by the child class
    def initializeSystemData(self):
        pass
    # endregion

    # region Functions used to retrieve information from the exchange
    def getCurrentPrice(self):
        try:
            self.SystemVariablesObj['CurrentPrice'] = self.ExchangeConnectionObj.fetch_ticker(
                        self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX]
            )['bid']
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

    # region Function used to retrieve algorithm configurations from database
    def getAlgorithmNames(self):
        # print("get Algorithm Names")
        QueryStr = """Select AlgorithmName From AlgorithmConfiguration"""

        QueryData = (
        )

        AlgorithmNameArr = self.templateDatabaseRetriever(QueryStr, QueryData, "getAlgorithmNames")
        if AlgorithmNameArr is None:
            return
        return AlgorithmNameArr

    def getAlgorithmConfigurationObj(self, AlgorithmNameStr):
        # print("get Algorithm Configuration Object")
        QueryStr = """Select * From AlgorithmConfiguration WHERE AlgorithmName = %s"""

        QueryData = (
            AlgorithmNameStr,
        )

        AlgorithmConfigurationObj = self.templateDatabaseRetriever(QueryStr, QueryData, "getAlgorithmConfigurationObj")
        if AlgorithmConfigurationObj is None:
            return
        return AlgorithmConfigurationObj[0]
    # endregion
