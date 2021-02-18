from FrameworkBaseClasses.TraderBaseClass import TraderBaseClass
from assets import constants as Constant

from datetime import datetime
import sys as SystemObj
from time import sleep


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
        if self.IndicatorsObj['EMA_RETEST']['retest_candle_count'] is None:
            return
        if 'LastEmaRetestCount' not in self.CustomVariables:
            self.CustomVariables['LastEmaRetestCount'] = 0

        if 'OpenPositions' not in self.CustomVariables:
            self.CustomVariables['OpenPositions'] = []

        self.OpenOrderCountInt = self.countOpenOrders()
        self.OpenPositionCountInt = float(self.checkPosition())
        PositionSizeInt = format(self.getOrderQuantity(), '.4f')

        if self.OpenPositionCountInt != 0 and len(self.CustomVariables['OpenPositions']) == 0:
            if self.OpenPositionCountInt > 0:
                self.CustomVariables['OpenPositions'].append({'OrderSide': 'buy', 'PositionPrice': self.CurrentSystemVariables['CurrentPrice'], 'PositionSize': PositionSizeInt, 'Status': 'New', 'RetestCount': self.CustomVariables['LastEmaRetestCount']})
            else:
                self.CustomVariables['OpenPositions'].append({'OrderSide': 'sell', 'PositionPrice': self.CurrentSystemVariables['CurrentPrice'], 'PositionSize': PositionSizeInt, 'Status': 'New', 'RetestCount': self.CustomVariables['LastEmaRetestCount']})

        for i in range(0, len(self.CustomVariables['OpenPositions'])):
            if self.CustomVariables['OpenPositions'][i]['OrderSide'] == 'buy' and self.IndicatorsObj['EMA']['value'] <= self.CustomVariables['OpenPositions'][i]['PositionPrice'] and self.CustomVariables['OpenPositions'][i]['Status'] == 'New':
                if self.placeMarketOrder('sell', self.CustomVariables['OpenPositions'][i]['PositionSize']):
                    self.CustomVariables['OpenPositions'][i]['Status'] = 'Closed'

        if self.CustomVariables['LastEmaRetestCount'] != self.IndicatorsObj['EMA_RETEST']['retest_candle_count']:
            self.CustomVariables['LastEmaRetestCount'] = self.IndicatorsObj['EMA_RETEST']['retest_candle_count']
            if self.CustomVariables['LastEmaRetestCount'] < 2:

                if len(self.CustomVariables['OpenPositions']) > 0:
                    self.CustomVariables['OpenPositions'] = []

                if self.OpenOrderCountInt > 0:
                    self.cancelAllOrders()

                if self.OpenPositionCountInt != 0:
                    if self.OpenPositionCountInt > 0:
                        self.placeMarketOrder('sell')
                    elif self.OpenPositionCountInt < 0:
                        self.placeMarketOrder('buy')
                return

        OpenedPositionsInt = len(self.CustomVariables['OpenPositions'])
        print('EMA Retest Count: ' + str(self.CustomVariables['LastEmaRetestCount']))
        print('Open Positions: ' + str(OpenedPositionsInt))
        print(self.CustomVariables['OpenPositions'])
        if OpenedPositionsInt > 0 and self.CustomVariables['OpenPositions'][OpenedPositionsInt-1]['RetestCount'] == self.CustomVariables['LastEmaRetestCount']:
            return

        # print("latest Ema Retest Count: " + str(self.CustomVariables['LastEmaRetestCount']))
        # print("BB: " + str(self.IndicatorsObj['BB']))
        # print("Current Price: " + str(self.CurrentSystemVariables['CurrentPrice']))
        # print("Algorithm Position Size: " + str(PositionSizeInt))

        if self.CurrentSystemVariables['CurrentPrice'] > self.IndicatorsObj['BB']['upper']:
            self.cancelAllOrders()
            self.OpenOrderCountInt = self.countOpenOrders()
            print('Entering Order Count Loop')
            while self.OpenOrderCountInt > 0:
                self.OpenOrderCountInt = self.countOpenOrders()
                sleep(0.01)

            print('Exited Order Count Loop')
            if self.placeMarketOrder('sell', PositionSizeInt):
                self.CustomVariables['OpenPositions'].append({'OrderSide': 'sell', 'PositionPrice': self.CurrentSystemVariables['CurrentPrice'], 'PositionSize': PositionSizeInt, 'Status': 'New', 'RetestCount': self.CustomVariables['LastEmaRetestCount']})
            self.OpenPositionCountInt = float(self.checkPosition())

            print('Entering Open Position Loop')
            while self.OpenPositionCountInt == 0:
                self.OpenPositionCountInt = float(self.checkPosition())
                sleep(0.01)
            print('Exiting Open Position Loop')

            print('New Position Size: ' + str(self.OpenPositionCountInt))
            self.placeClosingOrder('buy')
        elif self.CurrentSystemVariables['CurrentPrice'] < self.IndicatorsObj['BB']['lower']:
            self.cancelAllOrders()
            self.OpenOrderCountInt = self.countOpenOrders()
            print('Entering Order Count Loop')
            while self.OpenOrderCountInt > 0:
                self.OpenOrderCountInt = self.countOpenOrders()
                sleep(0.01)

            print('Exited Order Count Loop')
            if self.placeMarketOrder('buy', PositionSizeInt):
                self.CustomVariables['OpenPositions'].append({'OrderSide': 'buy', 'PositionPrice': self.CurrentSystemVariables['CurrentPrice'], 'PositionSize': PositionSizeInt, 'Status': 'New', 'RetestCount': self.CustomVariables['LastEmaRetestCount']})

            self.OpenPositionCountInt = float(self.checkPosition())
            print('Entering Open Position Loop')
            while self.OpenPositionCountInt == 0:
                self.OpenPositionCountInt = float(self.checkPosition())
                sleep(0.01)
            print('Exiting Open Position Loop')

            print('New Position Size: ' + str(self.OpenPositionCountInt))
            self.placeClosingOrder('sell')
        elif self.OpenPositionCountInt != 0:
            if self.OpenPositionCountInt > 0:
                self.placeClosingOrder('sell')
            elif self.OpenPositionCountInt < 0:
                self.placeClosingOrder('buy')

        # region Handling actions based on the trading state of the algorithm
        # trading state is managed by the risk management thread
        if not self.checkTradingState():
            return
        # endregion

        pass

    def priceDataGeneration(self):
        self.getCurrentPrice()
