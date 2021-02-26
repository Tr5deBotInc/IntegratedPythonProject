from FrameworkBaseClasses.ManagerBaseClass import ManagerBaseClass
from FrameworkImplementations.IndicatorGenerationClass import IndicatorGenerationClass
from FrameworkImplementations.RiskManagementClass import RiskManagementClass
from FrameworkImplementations.TraderClass import TraderClass
from assets import constants as Constant
from assets import ProjectFunctions

from datetime import datetime, timedelta
import sys as SystemObj
import time
from threading import Timer


class ManagerClass(ManagerBaseClass):
    FiveMinCandleArr = []
    CurrentSimpleMovingAverageFloat = None
    CurrentExponentialMovingAverageRetestInt = {
        'prev_EMA': None,
        'prev_candle': None,
        'retest_candle_count': None,
        'placement': None
    }
    CurrentExponentialMovingAverageObj = {
        'value': None
    }
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
    CloseOrderCountObj = {
        'OrderCount': None,
        'RetestCount': None
    }

    def __init__(self):
        # print("Manager Class Constructor")
        super().__init__()
        self.initializeProcessObjects()

        self.startProcessThreading()

    def initializeProcessObjects(self):
        # print("Initializing Process Objects")
        AlgorithmNameStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]

        IndicatorGenerationObj = IndicatorGenerationClass()
        IndicatorGenerationObj.setAlgorithmConfigurationObj(self.AlgorithmConfigurationObj)
        IndicatorGenerationObj.setExchangeConnectionObj(self.ExchangeConnectionObj)
        IndicatorGenerationObj.setDatabaseConnectionDetailsObj(self.DatabaseConnectionDetails)
        IndicatorGenerationObj.setSystemVariables(self.SystemVariablesObj)
        IndicatorGenerationObj.setIndicators({
            'BB': self.BollingerBandObj,
            'RSI': self.RsiBandObj,
            'SMA': self.CurrentSimpleMovingAverageFloat,
            'EMA': self.CurrentExponentialMovingAverageObj,
            'EMA_RETEST': self.CurrentExponentialMovingAverageRetestInt,
            'COC': self.CloseOrderCountObj,
            'TimeStamp': self.IndicatorTimeStampObj
        })
        IndicatorGenerationObj.setCandleArr({
            'FiveMinuteCandles': self.FiveMinCandleArr
        })

        RiskManagementObj = RiskManagementClass()
        RiskManagementObj.setAlgorithmConfigurationObj(self.AlgorithmConfigurationObj)
        RiskManagementObj.setExchangeConnectionObj(self.ExchangeConnectionObj)
        RiskManagementObj.setDatabaseConnectionDetailsObj(self.DatabaseConnectionDetails)
        RiskManagementObj.setSystemVariables(self.SystemVariablesObj)
        RiskManagementObj.setExchangeConnectionDetailsObj(self.ExchangeConnectionDetails)
        RiskManagementObj.setIndicators({
            'BB': self.BollingerBandObj,
            'RSI': self.RsiBandObj,
            'SMA': self.CurrentSimpleMovingAverageFloat,
            'EMA': self.CurrentExponentialMovingAverageObj,
            'EMA_RETEST': self.CurrentExponentialMovingAverageRetestInt,
            'COC': self.CloseOrderCountObj,
            'TimeStamp': self.IndicatorTimeStampObj
        })

        TraderObj = TraderClass()
        TraderObj.setExchangeConnectionObj(self.ExchangeConnectionObj)
        TraderObj.setAlgorithmConfigurationObj(self.AlgorithmConfigurationObj)
        TraderObj.setExchangeConnectionDetailsObj(self.ExchangeConnectionDetails)
        TraderObj.setIndicators({
            'BB': self.BollingerBandObj,
            'RSI': self.RsiBandObj,
            'SMA': self.CurrentSimpleMovingAverageFloat,
            'EMA': self.CurrentExponentialMovingAverageObj,
            'EMA_RETEST': self.CurrentExponentialMovingAverageRetestInt,
            'COC': self.CloseOrderCountObj,
            'TimeStamp': self.IndicatorTimeStampObj
        })
        TraderObj.setSystemVariables(self.SystemVariablesObj)
        TraderObj.setDatabaseConnectionDetailsObj(self.DatabaseConnectionDetails)

        if AlgorithmNameStr == Constant.PRICE_DATA_GENERATION_BASE_VERSION:
            self.ThreadInstantiationArr = [
                {'ProcessObj': TraderObj, 'IntervalInt': 1},
                {'ProcessObj': self, 'IntervalInt': 1}
            ]
        else:
            IndicatorGenerationIntervalInt = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_CANDLE_DURATION_INDEX] * 60
            self.ThreadInstantiationArr = [
                {'ProcessObj': IndicatorGenerationObj, 'IntervalInt': IndicatorGenerationIntervalInt},
                {'ProcessObj': RiskManagementObj, 'IntervalInt': 60},
                {'ProcessObj': TraderObj, 'IntervalInt': 10},
                {'ProcessObj': self, 'IntervalInt': 1}
            ]

    def initializeSystemData(self):
        AlgorithmNameStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]
        if AlgorithmNameStr == Constant.PRICE_DATA_GENERATION_BASE_VERSION:
            return
        # region Indicator Initialization
        CandleDurationInt = \
            int(self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_CANDLE_DURATION_INDEX])
        FrameCountInt = \
            int(self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_FRAME_COUNT_INDEX])

        BollingerBandUsedBool = False
        if self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_BB_STANDARD_DEVIATION_INDEX] is not None:
            BollingerBandUsedBool = True

        RsiUsedBool = False
        if self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_RSI_LOWER_INTENSITY_INDEX] is not None and self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_RSI_UPPER_INTENSITY_INDEX] is not None:
            RsiUsedBool = True

        CandlestickDataArr = self.get1mCandles(CandleDurationInt, FrameCountInt)
        for iterator in range(0, len(CandlestickDataArr), CandleDurationInt):
            self.FiveMinCandleArr.append({
                'mid': (CandlestickDataArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX] +
                        CandlestickDataArr[iterator + CandleDurationInt-1][Constant.CANDLE_CLOSING_PRICE_INDEX])/2,
                'open': CandlestickDataArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX],
                'close': CandlestickDataArr[iterator + CandleDurationInt-1][Constant.CANDLE_CLOSING_PRICE_INDEX],
                'time_stamp':
                    CandlestickDataArr[iterator + CandleDurationInt-1][Constant.CANDLE_TIMESTAMP_INDEX]
            })

        if BollingerBandUsedBool:
            BollingerBandObj = ProjectFunctions.getBollingerBands(self.FiveMinCandleArr, self.AlgorithmConfigurationObj)
            if 'upper' in BollingerBandObj and 'lower' in BollingerBandObj and\
                    ProjectFunctions.checkIfNumber(BollingerBandObj['upper']) and\
                    ProjectFunctions.checkIfNumber(BollingerBandObj['lower']):
                self.createIndicatorUpdateLog(self.ProcessName,  datetime.now(), 'Bollinger Band', BollingerBandObj, 'True')
                self.BollingerBandObj = BollingerBandObj
            else:
                self.createIndicatorUpdateLog(self.ProcessName, datetime.now(), 'Bollinger Band', {}, 'False')

        if RsiUsedBool:
            RsiBandObj = ProjectFunctions.getRsiBands(self.FiveMinCandleArr, self.AlgorithmConfigurationObj)
            if 'upper' in RsiBandObj and 'lower' in RsiBandObj and\
                    ProjectFunctions.checkIfNumber(RsiBandObj['upper']) and\
                    ProjectFunctions.checkIfNumber(RsiBandObj['lower']):
                self.createIndicatorUpdateLog(self.ProcessName, datetime.now(), 'RSI Band', RsiBandObj, 'True')
                self.RsiBandObj = RsiBandObj
            else:
                self.createIndicatorUpdateLog(self.ProcessName, datetime.now(), 'RSI Band', {}, 'False')

        self.CloseOrderCountObj = {
            'OrderCount': 0,
            'RetestCount': 0
        }
        self.createIndicatorUpdateLog(self.ProcessName, datetime.now(), 'COC', self.CloseOrderCountObj, 'True')

        self.CurrentSimpleMovingAverageFloat = ProjectFunctions.getSimpleMovingAverage(self.FiveMinCandleArr)
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
        self.getCurrentPosition()
        # endregion

    def initiateExecution(self):
        # this will be changed to a function that logs the one second price to the database
        self.getCurrentPrice()
        self.getCurrentBalance()
        self.getCurrentPosition()
