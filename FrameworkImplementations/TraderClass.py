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
        elif AlgorithmNameStr == Constant.BB_RSI_ANALYSER_BASE_VERSION:
            self.bbRsiTradingAlgorithm()
        else:
            print('Trading algorithm ' + AlgorithmNameStr + 'was not found')
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

        if self.CurrentSystemVariables['TradingState'] == 'Market Halt':
            if self.OpenPositionCountInt == 0 and self.OpenOrderCountInt > 0:
                self.cancelAllOrders()
                self.createProcessExecutionLog(self.ProcessName, datetime.now(),
                                               "Process Update: Closing all orders on Market Halt trading state")
                return
            elif self.OpenOrderCountInt == 0 and self.OpenPositionCountInt == 0:
                return
        elif self.CurrentSystemVariables['TradingState'] == 'Market Dead Stop' or \
                self.CurrentSystemVariables['TradingState'] == 'Manual Halt':
            if self.OpenPositionCountInt != 0:
                if self.OpenPositionCountInt > 0:
                    self.placeMarketOrder('sell')
                elif self.OpenPositionCountInt < 0:
                    self.placeMarketOrder('buy')

                self.createProcessExecutionLog(self.ProcessName, datetime.now(),
                                               "Process Update: Creating market orders on open position due to "
                                               + self.CurrentSystemVariables['TradingState'] + " trading state")
                if self.OpenOrderCountInt > 0:
                    self.cancelAllOrders()
                    self.createProcessExecutionLog(self.ProcessName, datetime.now(),
                                                   "Process Update: Closing all orders due to "
                                                   + self.CurrentSystemVariables['TradingState'] + " trading state")
            return
        elif self.CurrentSystemVariables['TradingState'] is None:
            # In case the algorithm configuration variables are not set yet, we do not execute trading functionality
            self.createProcessExecutionLog(self.ProcessName, datetime.now(),
                                           "Process Update: Algorithm trading state not set")
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

        pass