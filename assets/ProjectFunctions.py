from assets import constants as Constant

import math


def getBollingerBands(CandleDataArr):
    # print("get Bollinger Bands")
    SimpleMovingAverageFloat = getSimpleMovingAverage(CandleDataArr, "getBollingerBands")
    StandardDeviationFloat = getStandardDeviationOfCandleArr(CandleDataArr)
    UpperBandFloat = SimpleMovingAverageFloat['value'] + (StandardDeviationFloat * Constant.BB_STANDARD_DEVIATION)
    LowerBandFloat = SimpleMovingAverageFloat['value'] - (StandardDeviationFloat * Constant.BB_STANDARD_DEVIATION)
    BollingerBandObj = {
        'upper': UpperBandFloat,
        'lower': LowerBandFloat
    }

    return BollingerBandObj


def getRsiBands(CandleDataArr):
    # print("get RSI Bands")
    ArrLengthInt = len(CandleDataArr)
    CandlePositiveChangeArr = []
    CandleNegativeChangeArr = []

    for CandleObj in CandleDataArr:
        if CandleObj['open'] <= CandleObj['close']:
            CandlePositiveChangeArr.append(CandleObj['close'] - CandleObj['open'])
        else:
            CandleNegativeChangeArr.append(CandleObj['open'] - CandleObj['close'])
    AveragePositiveChangeFloat = 0
    AverageNegativeChangeFloat = 0

    if len(CandlePositiveChangeArr) > 0:
        AveragePositiveChangeFloat = sum(CandlePositiveChangeArr)/ArrLengthInt
    if len(CandleNegativeChangeArr) > 0:
        AverageNegativeChangeFloat = sum(CandleNegativeChangeArr)/ArrLengthInt

    SimpleMovingAverageFloat = getSimpleMovingAverage(CandleDataArr, "getRsiBands")

    AverageGain = (AveragePositiveChangeFloat/Constant.RSI_UPPER_BAND_INTENSITY) * \
                  (100-Constant.RSI_UPPER_BAND_INTENSITY)

    AverageLoss = (AverageNegativeChangeFloat * Constant.RSI_LOWER_BAND_INTENSITY) / \
                  (100 - Constant.RSI_LOWER_BAND_INTENSITY)

    UpperBandFloat = SimpleMovingAverageFloat['value'] + AverageGain * Constant.INDICATOR_FRAME_COUNT
    LowerBandFloat = SimpleMovingAverageFloat['value'] - AverageLoss * Constant.INDICATOR_FRAME_COUNT

    RsiBandObj = {
        'upper': UpperBandFloat,
        'lower': LowerBandFloat
    }

    return RsiBandObj


def getSimpleMovingAverage(CandleDataArr, FunctionNameStr="not given"):
    # print("get Simple Moving Average")
    CandleArrSumFloat = 0
    for CandleObj in CandleDataArr:
        CandleArrSumFloat += CandleObj['mid']
    SimpleMovingAverageFloat = CandleArrSumFloat / len(CandleDataArr)

    SimpleMovingAverageObj = {
        'value': SimpleMovingAverageFloat
    }
    return SimpleMovingAverageObj


def getStandardDeviationOfCandleArr(CandleDataArr):
    # print("get StandardDeviation Of Candle Arr")
    VarianceFloat = 0.0
    CandleArrSumFloat = 0
    for CandleObj in CandleDataArr:
        CandleArrSumFloat += CandleObj['mid']

    AverageFloat = CandleArrSumFloat/len(CandleDataArr)

    for CandleObj in CandleDataArr:
        VarianceFloat += pow(float(CandleObj['mid']) - float(AverageFloat), 2)

    return float(math.sqrt(VarianceFloat/len(CandleDataArr)))


def checkIfNumber(MixedVariable):
    if type(MixedVariable) == int or type(MixedVariable) == float:
        return True
    else:
        return False


def truncateFloat(InputFloat, DecimalPlacesInt):
    s = '{}'.format(InputFloat)
    if 'e' in s or 'E' in s:
        return '{0:.{1}f}'.format(InputFloat, DecimalPlacesInt)
    i, p, d = s.partition('.')
    return float('.'.join([i, (d + '0' * DecimalPlacesInt)[:DecimalPlacesInt]]))
