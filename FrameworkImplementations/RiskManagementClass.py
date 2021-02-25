from FrameworkBaseClasses.RiskManagementBaseClass import RiskManagementBaseClass
from assets import constants as Constant


class RiskManagementClass(RiskManagementBaseClass):
    def __init__(self):
        # print("Risk Management Class Constructor")
        super().__init__()

    def initiateExecution(self):
        AlgorithmNameStr = self.AlgorithmConfigurationObj[Constant.ALGORITHM_CONFIGURATION_ALGORITHM_NAME_INDEX]
        if AlgorithmNameStr == Constant.BB_RSI_ANALYSER_15_MINUTE_CANDLES_IMPROVED:
            self.determineTradingStateBbRsiImproved()
        elif AlgorithmNameStr == Constant.BB_RSI_ANALYSER_V3:
            self.determineTradingStateBbRsiV3()
        else:
            self.getAlgorithmTradingState()

    def determineTradingStateBbRsiImproved(self):
        # print("get Algorithm Trading State")
        AlgorithmRiskManagementLimitInt = 12
        QueryStr = """Select * From AlgorithmConfiguration Where AlgorithmName = %s"""

        QueryData = (
            self.CurrentSystemVariables['AlgorithmId'],
        )

        AlgorithmConfigurationObjArr = self.templateDatabaseRetriever(QueryStr, QueryData,
                                                                      "determineTradingStateBbRsiImproved")
        if AlgorithmConfigurationObjArr is None or len(AlgorithmConfigurationObjArr) != 1:
            return
        AlgorithmTradingState = AlgorithmConfigurationObjArr[0][Constant.ALGORITHM_CONFIGURATION_TRADING_STATE_INDEX]

        if AlgorithmTradingState == 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = 'Manual Halt'
            return
        elif AlgorithmTradingState != 'Manual Halt' and \
                self.CurrentSystemVariables['TradingState'] == 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = AlgorithmTradingState

        if self.IndicatorsObj['COC']['OrderCount'] >= AlgorithmRiskManagementLimitInt and AlgorithmTradingState != 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = 'Market Dead Stop'
        elif self.IndicatorsObj['COC']['OrderCount'] < AlgorithmRiskManagementLimitInt and AlgorithmTradingState == 'Market Dead Stop':
            self.CurrentSystemVariables['TradingState'] = 'Active'

        if AlgorithmTradingState != self.CurrentSystemVariables['TradingState']:
            if self.CurrentSystemVariables['TradingState'] is None:
                self.CurrentSystemVariables['TradingState'] = AlgorithmTradingState
            self.setAlgorithmTradingState(self.CurrentSystemVariables['TradingState'])

    def determineTradingStateBbRsiV3(self):
        # print("get Algorithm Trading State")
        AlgorithmRiskManagementLimitInt = 12
        QueryStr = """Select * From AlgorithmConfiguration Where AlgorithmName = %s"""

        QueryData = (
            self.CurrentSystemVariables['AlgorithmId'],
        )

        AlgorithmConfigurationObjArr = self.templateDatabaseRetriever(QueryStr, QueryData,
                                                                      "determineTradingStateBbRsiImproved")
        if AlgorithmConfigurationObjArr is None or len(AlgorithmConfigurationObjArr) != 1:
            return
        AlgorithmTradingState = AlgorithmConfigurationObjArr[0][Constant.ALGORITHM_CONFIGURATION_TRADING_STATE_INDEX]

        if AlgorithmTradingState == 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = 'Manual Halt'
            return
        elif AlgorithmTradingState != 'Manual Halt' and \
                self.CurrentSystemVariables['TradingState'] == 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = AlgorithmTradingState

        if self.IndicatorsObj['COC']['OrderCount'] >= AlgorithmRiskManagementLimitInt and AlgorithmTradingState != 'Manual Halt':
            self.CurrentSystemVariables['TradingState'] = 'Reverse'
        elif self.IndicatorsObj['COC']['OrderCount'] < AlgorithmRiskManagementLimitInt and AlgorithmTradingState == 'Market Dead Stop':
            self.CurrentSystemVariables['TradingState'] = 'Active'

        if AlgorithmTradingState != self.CurrentSystemVariables['TradingState']:
            if self.CurrentSystemVariables['TradingState'] is None:
                self.CurrentSystemVariables['TradingState'] = AlgorithmTradingState
            self.setAlgorithmTradingState(self.CurrentSystemVariables['TradingState'])

    # endregion
