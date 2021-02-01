from FrameworkBaseClasses.IndicatorGenerationBaseClass import IndicatorGenerationBaseClass
from assets import ProjectFunctions
from assets import constants as Constant

from datetime import datetime


class IndicatorGenerationClass(IndicatorGenerationBaseClass):
    def __init__(self):
        # print("Indicator Generation Class Constructor")
        self.ObjectTypeValidationArr = {
            'DatabaseDetails': ['ServerName', 'DatabaseName', 'UserName', 'Password'],
            'Indicators': ['BB', 'RSI', 'SMA', 'TimeStamp'],
            'CandleArr': ['FiveMinuteCandles']
        }
        super().__init__()

    def initiateExecution(self):
        self.updateCandleArr()
        self.updateBollingerBandIndicator()
        self.updateRsiBandIndicator()
        self.updateSmaIndicator()

        self.IndicatorsObj['TimeStamp']['datetime'] = datetime.now()

    def updateCandleArr(self):
        CandleDurationInt = \
            int(self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_INDICATOR_CANDLE_DURATION_INDEX])

        OutstandingCandlestickArr = self.get1mCandles(
            self.CandleArr['FiveMinuteCandles'][len(self.CandleArr['FiveMinuteCandles']) - 1]['time_stamp'] + 1000)
        if len(OutstandingCandlestickArr) < 5:
            return

        for iterator in range(0, len(OutstandingCandlestickArr), CandleDurationInt):
            if len(OutstandingCandlestickArr) < iterator + CandleDurationInt:
                break
            self.CandleArr['FiveMinuteCandles'].append({
                'mid': (OutstandingCandlestickArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX] +
                        OutstandingCandlestickArr[iterator + CandleDurationInt - 1][
                            Constant.CANDLE_CLOSING_PRICE_INDEX]) / 2,
                'open': OutstandingCandlestickArr[iterator][Constant.CANDLE_OPEN_PRICE_INDEX],
                'close': OutstandingCandlestickArr[iterator + CandleDurationInt - 1][
                    Constant.CANDLE_CLOSING_PRICE_INDEX],
                'time_stamp': OutstandingCandlestickArr[iterator + CandleDurationInt - 1][
                    Constant.CANDLE_TIMESTAMP_INDEX]
            })

            self.CandleArr['FiveMinuteCandles'].pop(0)

    def updateBollingerBandIndicator(self):
        BollingerBandObj = ProjectFunctions.getBollingerBands(self.CandleArr['FiveMinuteCandles'], self.AlgorithmConfigurationObj)
        if 'upper' in BollingerBandObj and 'lower' in BollingerBandObj and \
                ProjectFunctions.checkIfNumber(BollingerBandObj['upper']) and \
                ProjectFunctions.checkIfNumber(BollingerBandObj['lower']):
            self.IndicatorsObj['BB']['upper'] = BollingerBandObj['upper']
            self.IndicatorsObj['BB']['lower'] = BollingerBandObj['lower']
            self.createIndicatorUpdateLog(self.ProcessName,  datetime.now(), 'Bollinger Band', BollingerBandObj, 'True')
        else:
            self.createIndicatorUpdateLog(self.ProcessName,  datetime.now(), 'Bollinger Band', {}, 'False')

    def updateRsiBandIndicator(self):
        RsiBandObj = ProjectFunctions.getRsiBands(self.CandleArr['FiveMinuteCandles'], self.AlgorithmConfigurationObj)
        if 'upper' in RsiBandObj and 'lower' in RsiBandObj and \
                ProjectFunctions.checkIfNumber(RsiBandObj['upper']) and \
                ProjectFunctions.checkIfNumber(RsiBandObj['lower']):
            self.IndicatorsObj['RSI']['upper'] = RsiBandObj['upper']
            self.IndicatorsObj['RSI']['lower'] = RsiBandObj['lower']
            self.createIndicatorUpdateLog(self.ProcessName, datetime.now(), 'RSI Band', RsiBandObj, 'True')
        else:
            self.createIndicatorUpdateLog(self.ProcessName, datetime.now(), 'RSI Band', {}, 'False')

    def updateSmaIndicator(self):
        if ProjectFunctions.checkIfNumber(self.IndicatorsObj['SMA']['value']):
            SimpleMovingAverageObj = ProjectFunctions.getSimpleMovingAverage(self.CandleArr['FiveMinuteCandles'])
            self.IndicatorsObj['SMA']['value'] = SimpleMovingAverageObj['value']
            self.createIndicatorUpdateLog(self.ProcessName, datetime.now(), 'SMA',
                                          {'SMA': self.IndicatorsObj['SMA']}, 'True')
        else:
            self.createIndicatorUpdateLog(self.ProcessName, datetime.now(), 'SMA',
                                          {}, 'False')
