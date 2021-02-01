from FrameworkBaseClasses.ManagerBaseClass import ManagerBaseClass
from FrameworkImplementations.IndicatorGenerationClass import IndicatorGenerationClass
from FrameworkImplementations.RiskManagementClass import RiskManagementClass
from FrameworkImplementations.TraderClass import TraderClass
from assets import constants as Constant
from assets import ProjectFunctions

import time
from datetime import datetime


class ManagerClass(ManagerBaseClass):
    FiveMinCandleArr = []
    CurrentSimpleMovingAverageFloat = None
    BollingerBandObj = {
        'upper': None,
        'lower': None
    }
    RsiBandObj = {
        'upper': None,
        'lower': None
    }
    IndicatorTimeStampObj = {
        'datetime': None
    }

    def __init__(self):
        # print("Manager Class Constructor")
        super().__init__()
        self.initializeProcessObjects()
        self.startProcessThreading()

    def initializeProcessObjects(self):
        # print("Initializing Process Objects")
        IndicatorGenerationObj = IndicatorGenerationClass()
        IndicatorGenerationObj.setExchangeConnectionObj(self.ExchangeConnectionObj)
        IndicatorGenerationObj.setDatabaseConnectionDetailsObj(self.DatabaseConnectionDetails)
        IndicatorGenerationObj.setIndicators({
            'BB': self.BollingerBandObj,
            'RSI': self.RsiBandObj,
            'SMA': self.CurrentSimpleMovingAverageFloat,
            'TimeStamp': self.IndicatorTimeStampObj
        })
        IndicatorGenerationObj.setCandleArr({
            'FiveMinuteCandles': self.FiveMinCandleArr
        })

        RiskManagementObj = RiskManagementClass()
        RiskManagementObj.setExchangeConnectionObj(self.ExchangeConnectionObj)
        RiskManagementObj.setDatabaseConnectionDetailsObj(self.DatabaseConnectionDetails)
        RiskManagementObj.setSystemVariables(self.SystemVariablesObj)
        RiskManagementObj.setExchangeConnectionDetailsObj(self.ExchangeConnectionDetails)

        TraderObj = TraderClass()
        TraderObj.setExchangeConnectionObj(self.ExchangeConnectionObj)
        TraderObj.setExchangeConnectionDetailsObj(self.ExchangeConnectionDetails)
        TraderObj.setIndicators({
            'BB': self.BollingerBandObj,
            'RSI': self.RsiBandObj,
            'SMA': self.CurrentSimpleMovingAverageFloat,
            'TimeStamp': self.IndicatorTimeStampObj
        })
        TraderObj.setSystemVariables(self.SystemVariablesObj)
        TraderObj.setDatabaseConnectionDetailsObj(self.DatabaseConnectionDetails)

        self.ThreadInstantiationArr = [
            {'ProcessObj': IndicatorGenerationObj, 'IntervalInt': 300},
            {'ProcessObj': RiskManagementObj, 'IntervalInt': 60},
            {'ProcessObj': TraderObj, 'IntervalInt': 10},
            {'ProcessObj': self, 'IntervalInt': 1}
        ]

    def initializeSystemData(self):
        # region Indicator Initialization
        CandlestickDataArr = self.get1mCandles(Constant.INDICATOR_CANDLE_DURATION*Constant.INDICATOR_FRAME_COUNT)
        for iterator in range(0, len(CandlestickDataArr), Constant.INDICATOR_CANDLE_DURATION):
            self.FiveMinCandleArr.append({
                'mid': (CandlestickDataArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX] +
                        CandlestickDataArr[iterator + Constant.INDICATOR_CANDLE_DURATION-1][Constant.CANDLE_CLOSING_PRICE_INDEX])/2,
                'open': CandlestickDataArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX],
                'close': CandlestickDataArr[iterator + Constant.INDICATOR_CANDLE_DURATION-1][Constant.CANDLE_CLOSING_PRICE_INDEX],
                'time_stamp': CandlestickDataArr[iterator + Constant.INDICATOR_CANDLE_DURATION-1][Constant.CANDLE_TIMESTAMP_INDEX]
            })
        BollingerBandObj = ProjectFunctions.getBollingerBands(self.FiveMinCandleArr)
        if 'upper' in BollingerBandObj and 'lower' in BollingerBandObj and\
                ProjectFunctions.checkIfNumber(BollingerBandObj['upper']) and\
                ProjectFunctions.checkIfNumber(BollingerBandObj['lower']):
            self.createIndicatorUpdateLog(self.ProcessName,  datetime.now(), 'Bollinger Band', BollingerBandObj, 'True')
            self.BollingerBandObj = BollingerBandObj
        else:
            self.createIndicatorUpdateLog(self.ProcessName, datetime.now(), 'Bollinger Band', {}, 'False')

        RsiBandObj = ProjectFunctions.getRsiBands(self.FiveMinCandleArr)
        if 'upper' in RsiBandObj and 'lower' in RsiBandObj and\
                ProjectFunctions.checkIfNumber(RsiBandObj['upper']) and\
                ProjectFunctions.checkIfNumber(RsiBandObj['lower']):
            self.createIndicatorUpdateLog(self.ProcessName, datetime.now(), 'RSI Band', RsiBandObj, 'True')
            self.RsiBandObj = RsiBandObj
        else:
            self.createIndicatorUpdateLog(self.ProcessName, datetime.now(), 'RSI Band', {}, 'False')

        self.CurrentSimpleMovingAverageFloat = ProjectFunctions.getSimpleMovingAverage(self.FiveMinCandleArr, "initializeSystemData")
        self.IndicatorTimeStampObj = {'datetime': datetime.now()}

        if ProjectFunctions.checkIfNumber(self.CurrentSimpleMovingAverageFloat['value']):
            self.createIndicatorUpdateLog(self.ProcessName, self.IndicatorTimeStampObj['datetime'], 'SMA',
                                          {'SMA': self.CurrentSimpleMovingAverageFloat['value']}, 'True')
        else:
            self.createIndicatorUpdateLog(self.ProcessName, self.IndicatorTimeStampObj['datetime'], 'SMA',
                                          {}, 'False')
        # endregion
        # region State Variable Initialization
        self.getCurrentPrice()
        self.getCurrentBalance()
        # endregion

    def initiateExecution(self):
        # this will be changed to a function that logs the one second price to the database
        self.getCurrentPrice()
        self.getCurrentBalance()
