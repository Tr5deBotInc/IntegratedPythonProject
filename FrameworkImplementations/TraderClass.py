from FrameworkBaseClasses.TraderBaseClass import TraderBaseClass
from assets import constants as Constant

from datetime import datetime
import sys as SystemObj


class TraderClass(TraderBaseClass):

    def __init__(self):
        # print("Trader Class Constructor")
        super().__init__()

    def initiateExecution(self):
        AlgorithmNameStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]

        if AlgorithmNameStr == Constant.EMA_21_ANALYSER_BASE_VERSION:
            self.ema21AnalyzerAlgorithm()
        elif AlgorithmNameStr == Constant.BB_RSI_ANALYSER_BASE_VERSION or AlgorithmNameStr == Constant.BB_RSI_ANALYSER_15_MINUTE_CANDLES:
            self.bbRsiTradingAlgorithm()
        elif AlgorithmNameStr == Constant.PRICE_DATA_GENERATION_BASE_VERSION:
            self.priceDataGeneration()
        else:
            print('Trading algorithm ' + AlgorithmNameStr + ' was not found')
            SystemObj.exit()

    def bbRsiTradingAlgorithm(self):
        self.OpenOrderCountInt = self.countOpenOrders()
        self.OpenPositionCountInt = float(self.checkPosition())
        if self.OpenOrderCountInt is False or self.OpenPositionCountInt is False:
            self.createProcessExecutionLog(self.ProcessName, datetime.now(),
                                           "Process Update: Not executing trading functionality due to issues with"
                                           " getting Order Count or Position Count")
            return

        # region Handling actions based on the trading state of the algorithm
        # trading state is managed by the risk management thread
        if not self.checkTradingState():
            return
        # endregion

        # region Actual Algorithm
        self.createProcessExecutionLog(self.ProcessName, datetime.now(),
                                       "Process Update: Got to the start of the actual Algorithm. Open Position Count: "
                                       + str(self.OpenPositionCountInt) + ". Open Order Count: " + str(
                                           self.OpenOrderCountInt))

        self.createProcessExecutionLog(self.ProcessName, datetime.now(),
                                       "Indicators: " + str(self.IndicatorsObj))
        if self.OpenOrderCountInt < 1:
            if self.OpenPositionCountInt > 0:
                self.placeClosingOrder('sell')
                return
            if self.OpenPositionCountInt < 0:
                self.placeClosingOrder('buy')
                return
            self.placeOpeningOrders()
            return

        for CurrentOrder in self.CurrentOrderArr:
            if self.OpenPositionCountInt < 0:
                if CurrentOrder['side'] != 'buy' or CurrentOrder['price'] != round(self.IndicatorsObj['SMA']['value']):
                    if self.cancelAllOrders() is True:
                        self.placeClosingOrder('buy')
                    return
            elif self.OpenPositionCountInt > 0:
                if CurrentOrder['side'] != 'sell' or CurrentOrder['price'] != round(self.IndicatorsObj['SMA']['value']):
                    if self.cancelAllOrders() is True:
                        self.placeClosingOrder('sell')
                    return
            elif CurrentOrder['side'] == 'sell':
                UpperLimitArr = [self.IndicatorsObj['BB']['upper'], self.IndicatorsObj['RSI']['upper']]
                if CurrentOrder['price'] != round(max(UpperLimitArr)):
                    if self.cancelAllOrders() is True:
                        self.placeOpeningOrders()
                    return
            elif CurrentOrder['side'] == 'buy':
                LowerLimitArr = [self.IndicatorsObj['BB']['lower'], self.IndicatorsObj['RSI']['lower']]
                if CurrentOrder['price'] != round(min(LowerLimitArr)):
                    if self.cancelAllOrders() is True:
                        self.placeOpeningOrders()
                    return
        # endregion

    def ema21AnalyzerAlgorithm(self):
        if 'LastEmaRetestCount' not in self.CustomVariables:
            # setattr(self.CustomVariables, 'LastEmaRetestCount', self.IndicatorsObj['EMA_RETEST']['retest_candle_count'])
            self.CustomVariables['LastEmaRetestCount'] = self.IndicatorsObj['EMA_RETEST']['retest_candle_count']

        if 'StandardDeviation' not in self.CustomVariables:
            self.CustomVariables['StandardDeviation'] = 1

        if self.CustomVariables['LastEmaRetestCount'] != self.IndicatorsObj['EMA_RETEST']['retest_candle_count']:
            self.CustomVariables['LastEmaRetestCount'] = self.IndicatorsObj['EMA_RETEST']['retest_candle_count']
            if self.CustomVariables['LastEmaRetestCount'] < 21:
                return


        # region Handling actions based on the trading state of the algorithm
        # trading state is managed by the risk management thread
        if not self.checkTradingState():
            return
        # endregion

        pass

    def priceDataGeneration(self):
        self.getCurrentPrice()
