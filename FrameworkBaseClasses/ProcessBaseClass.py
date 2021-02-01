from assets import constants as Constant

import sys as SystemObj
import mysql.connector
from datetime import datetime, timezone


class ProcessBaseClass:
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
    AlgorithmConfigurationObj = None
    ProcessName = ""
    ObjectTypeValidationArr = []

    # region Variables provided by Manager class
    IndicatorsObj = None
    CurrentSystemVariables = None
    CandleArr = []
    # endregion

    def initiateExecution(self):
        # print("Initiating process execution flow")
        pass

    # region Functions used to validate and store variables provided by the manager class
    def setExchangeConnectionObj(self, Object):
        self.ExchangeConnectionObj = Object

    def setAlgorithmConfigurationObj(self, Object):
        self.AlgorithmConfigurationObj = Object

    def setExchangeConnectionDetailsObj(self, Object):
        if self.validateObject(Object, 'ExchangeDetails'):
            self.ExchangeConnectionDetails = Object
        else:
            print("Process Base Class for " + self.ProcessName + ": please provide valid Exchange Details")
            SystemObj.exit()

    def setDatabaseConnectionDetailsObj(self, Object):
        if self.validateObject(Object, 'DatabaseDetails'):
            self.DatabaseConnectionDetails = Object
        else:
            print("Process Base Class for " + self.ProcessName + ": please provide valid Database Details")
            SystemObj.exit()

    def setIndicators(self, Object):
        if self.validateObject(Object, 'Indicators'):
            self.IndicatorsObj = Object
        else:
            print("Process Base Class for " + self.ProcessName + ": please provide valid Indicator Object")
            SystemObj.exit()

    def setSystemVariables(self, Object):
        if self.validateObject(Object, 'SystemVariables'):
            self.CurrentSystemVariables = Object
        else:
            print("Process Base Class for " + self.ProcessName + ": please provide valid State Variable Object")
            SystemObj.exit()

    def setCandleArr(self, Object):
        if self.validateObject(Object, 'CandleArr'):
            self.CandleArr = Object
        else:
            print("Process Base Class for " + self.ProcessName + ": please provide valid Candle Arr")
            SystemObj.exit()

    def validateObject(self, Object, ObjectTypeStr):
        ValidationResultBool = True
        if ObjectTypeStr in self.ObjectTypeValidationArr:
            RequiredParametersArr = self.ObjectTypeValidationArr[ObjectTypeStr]
        else:
            return False

        for key in RequiredParametersArr:
            if key not in Object:
                ValidationResultBool = False

        return ValidationResultBool
    # endregion

    # region Base function used to retrieve information from the database
    def templateDatabaseRetriever(self, QueryStr, QueryData, FunctionNameStr=" "):
        try:
            ConnectionObj = mysql.connector.connect(host=self.DatabaseConnectionDetails['ServerName'],
                                                    database=self.DatabaseConnectionDetails['DatabaseName'],
                                                    user=self.DatabaseConnectionDetails['UserName'],
                                                    password=self.DatabaseConnectionDetails['Password'])

            CursorObj = ConnectionObj.cursor(buffered=True)
            CursorObj.execute(QueryStr, QueryData)
            RetrievedDataObj = CursorObj.fetchall()
            if ConnectionObj.is_connected():
                CursorObj.close()
                ConnectionObj.close()
            else:
                print("Failed to close MySQL connection")

            return RetrievedDataObj

        except mysql.connector.Error as error:
            print(
                self.ProcessName + " in " + FunctionNameStr + " failed to retrieve information from MySQL database {}".format(error))
            print(QueryStr)
            print(QueryData)
            print(datetime.now())
    # endregion

    # region Base function used to insert information into the database
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
    # endregion

    # region Functions used to long process successes and failures as system executes
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

    def createIndicatorUpdateLog(self, ProcessNameStr, EntryDateTimeObj, IndicatorNameStr, IndicatorDataObj,
                                 SuccessStr):
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
            self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_TRADING_PAIR_SYMBOL_INDEX],
            EntryDateTimeObj,
            CurrencyPrice
        )

        self.templateDatabaseLogger(QueryStr, QueryData, "createPriceLogEntry")

    def createOrderLog(self, EntryDateTimeObj, OrderPriceFloat, OrderActionStr, OrderDirectionStr, OrderQuantityInt,
                       PortfolioValueFloat):
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
