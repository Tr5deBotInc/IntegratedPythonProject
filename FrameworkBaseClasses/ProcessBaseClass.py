import sys as SystemObj


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
    ProcessName = ""
    ObjectTypeValidationArr = []

    # region Variables provided by Manager class
    IndicatorsObj = None
    CurrentSystemVariables = None
    CandleArr = []
    # endregion

    def __init__(self):
        # print("Process Base Class Constructor")
        super().__init__()

    def initiateExecution(self):
        # print("Initiating process execution flow")
        pass

    def setExchangeConnectionObj(self, Object):
        self.ExchangeConnectionObj = Object

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

