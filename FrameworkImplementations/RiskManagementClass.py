from FrameworkBaseClasses.RiskManagementBaseClass import RiskManagementBaseClass


class RiskManagementClass(RiskManagementBaseClass):
    def __init__(self):
        # print("Risk Management Class Constructor")
        super().__init__()

    def initiateExecution(self):
        self.getAlgorithmTradingState()
        pass
