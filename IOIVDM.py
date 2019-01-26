from __future__ import print_function
import json
import urllib
import re
import os, sys
import datetime
import math
from math import *
import csv
import time
import boto3

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')


def lambda_handler(event, context):
    bucketin = event['Records'][0]['s3']['bucket']['name']
    keyin = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')
    bucketout = bucketin
    length = len(keyin)
    withstp = keyin[9:length - 7]
    lengthwstp = len(withstp)
    first5 = withstp[0:5]
    if (first5 == 'BANKN'):
        companyname = 'BANKNIFTY'
    else:
        companyname = 'NIFTY'
    lencompany = len(companyname)
    stp = withstp[lencompany:lengthwstp]
    keycall = keyin[:length - 7] + 'Call.csv'
    keyput = keyin[:length - 7] + 'Put.csv'
    keyivoicall = 'SampledData/' + companyname + 'Call.csv'
    keyivoiput = 'SampledData/' + companyname + 'Put.csv'
    keyprmtr = 'Controllers/PRMTR.csv'  # + keyin[26:length]
    keyout = 'MDIVOIDataOne/' + companyname + '/' + stp + '.csv'
    PRMTR = CSVtoPRMTRlmbda(bucketin, keyprmtr)
    LastDateTime = PRMTR[2][0]
    LastDateTime = datetime.datetime.strptime(LastDateTime, '%m/%d/%y %H:%M')
    CurrentDateTime = PRMTR[1][0]
    CurrentDateTime = datetime.datetime.strptime(CurrentDateTime, '%m/%d/%y %H:%M')
    nos = PRMTR[1][1]
    MatrixIvOiCall = IvOiCSVtoArraylmbda(bucketin, keyivoicall, stp)
    MatrixIvOiPut = IvOiCSVtoArraylmbda(bucketin, keyivoiput, stp)
    MatrixCall = TickCSVtoMatrixlmbda(bucketin, keycall, LastDateTime)
    MatrixPut = TickCSVtoMatrixlmbda(bucketin, keyput, LastDateTime)
    diffVolumePut = MatrixIvOiPut[10] - MatrixIvOiPut[16]
    diffOiPut = MatrixIvOiPut[11] - MatrixIvOiPut[17]
    diffVolumeCall = MatrixIvOiCall[10] - MatrixIvOiCall[16]
    diffOiCall = MatrixIvOiCall[11] - MatrixIvOiCall[17]
    ivcallask = implied_volatility(MatrixIvOiCall, 'Call', 'ask')
    ivcallbid = implied_volatility(MatrixIvOiCall, 'Call', 'bid')
    ivcalltrade = implied_volatility(MatrixIvOiCall, 'Call', 'trade')
    ivputask = implied_volatility(MatrixIvOiPut, 'Put', 'ask')
    ivputbid = implied_volatility(MatrixIvOiPut, 'Put', 'bid')
    ivputtrade = implied_volatility(MatrixIvOiPut, 'Put', 'trade')
    MatrixIvOiPut = [1 if x == 0 else x for x in MatrixIvOiPut]
    MatrixIvOiCall = [1 if x == 0 else x for x in MatrixIvOiCall]
    IvOiPCR = [x / y for x, y in zip(MatrixIvOiPut, MatrixIvOiCall)]
    try:
        ivPCRask = ivputask / ivcallask
    except Exception as e:
        ivPCRask = 1
    try:
        ivPCRbid = ivputbid / ivcallbid
    except Exception as e:
        ivPCRbid = 1
    try:
        ivPCRtrade = ivputtrade / ivcalltrade
    except Exception as e:
        ivPCRtrade = 1
    IvOiPCR.extend([ivPCRask])
    IvOiPCR.extend([ivPCRbid])
    IvOiPCR.extend([ivPCRtrade])
    IvOiPCR.extend([ivcallask])
    IvOiPCR.extend([ivcallbid])
    IvOiPCR.extend([ivcalltrade])
    IvOiPCR.extend([ivputask])
    IvOiPCR.extend([ivputbid])
    IvOiPCR.extend([ivputtrade])
    IvOiPCR.extend([diffVolumeCall])
    IvOiPCR.extend([diffOiCall])
    IvOiPCR.extend([diffVolumePut])
    IvOiPCR.extend([diffOiPut])
    InitToZero()
    MDCall = marketdynamic(MatrixCall)
    MDCall = [1 if x == 0 else x for x in MDCall]
    InitToZero()
    MDPut = marketdynamic(MatrixPut)
    MDPut = [1 if x == 0 else x for x in MDPut]
    MDPCR = [x / y for x, y in zip(MDPut, MDCall)]
    ValuePerTradeCall = MDCall[14]
    ValuePerBidCall = MDCall[29]
    ValuePerAskCall = MDCall[44]
    TradeFrequencyUpTickCall = MDCall[54]
    TradeFrequencyDownTickCall = MDCall[55]
    BidFrequencyUpTickCall = MDCall[84]
    BidFrequencyDownTickCall = MDCall[85]
    AskFrequencyUpTickCall = MDCall[114]
    AskFrequencyDownTickCall = MDCall[115]
    ValuePerTradePut = MDPut[14]
    ValuePerBidPut = MDPut[29]
    ValuePerAskPut = MDPut[44]
    TradeFrequencyUpTickPut = MDPut[54]
    TradeFrequencyDownTickPut = MDPut[55]
    BidFrequencyUpTickPut = MDPut[84]
    BidFrequencyDownTickPut = MDPut[85]
    AskFrequencyUpTickPut = MDPut[114]
    AskFrequencyDownTickPut = MDPut[115]
    addMD = [ValuePerTradeCall, ValuePerTradePut, ValuePerBidCall, ValuePerBidPut, ValuePerAskCall, ValuePerAskPut,
             TradeFrequencyUpTickCall, TradeFrequencyUpTickPut, TradeFrequencyDownTickCall, TradeFrequencyDownTickPut,
             BidFrequencyUpTickCall, BidFrequencyUpTickPut, BidFrequencyDownTickCall, BidFrequencyDownTickPut,
             AskFrequencyUpTickCall, AskFrequencyUpTickPut, AskFrequencyDownTickCall, AskFrequencyDownTickPut]
    MDPCR.extend(addMD)
    MDPCR.extend(IvOiPCR)
    Header = ['Open', 'TradePriceVolumeWeighted', 'TradeTotalPrice', 'Close', 'High', 'Low', 'TradeTickPrice', 'Volume',
              'TradeTickVolume', 'TradeMinVolume', 'TradeMaxVolume', 'TradeCurrentSize', 'TradeTotalSize',
              'TradeTotalValue', 'ValuePerTrade', 'TradeFrequency', 'BidCurrentPrice', 'BidTotalPrice',
              'BidPriceVolumeWeighted', 'BidMinPrice', 'BidMaxPrice', 'BidTickPrice', 'BidVolumePriceWeighted',
              'BidTickVolume', 'BidMinVolume', 'BidMaxVolume', 'BidCurrentSize', 'BidTotalSize', 'BidTotalValue',
              'ValuePerBid', 'BidFrequency', 'AskCurrentPrice', 'AskTotalPrice', 'AskPriceVolumeWeighted',
              'AskMinPrice', 'AskMaxPrice', 'AskTickPrice', 'AskVolumePriceWeighted', 'AskTickVolume', 'AskMinVolume',
              'AskMaxVolume', 'AskCurrentSize', 'AskTotalSize', 'AskTotalValue', 'ValuePerAsk', 'AskFrequency',
              'TradeTotalValueUpTick', ' TradeTotalValueDownTick', ' TradeTotalSizeUpTick', 'TradeTotalSizeDownTick',
              ' TradeTotalPriceUpTick', ' TradeTotalPriceDownTick', ' TradePriceVolumeWeightedUpTick',
              ' TradePriceVolumeWeightedDownTick', 'TradeFrequencyUpTick', ' TradeFrequencyDownTick',
              ' TradeMinPriceUpTick', ' TradeMinPriceDownTick', ' TradeMaxPriceUpTick', ' TradeMaxPriceDownTick',
              'TradeVolumePriceWeightedUpTick', ' TradeVolumePriceWeightedDownTick', ' TradeTickVolumeUpTick',
              ' TradeTickVolumeDownTick', ' TradeMinVolumeUpTick', ' TradeMinVolumeDownTick', 'TradeMaxVolumeUpTick',
              ' TradeMaxVolumeDownTick', ' ValuePerTradeUpTick', ' ValuePerTradeDownTick', ' TradeOpenPriceUpTick',
              ' TradeClosePriceUpTick', ' TradeOpenPriceDownTick', 'TradeClosePriceDownTick', ' TradeCloseVolumeUpTick',
              ' TradeCloseVolumeDownTick', 'BidTotalValueUpTick', ' BidTotalValueDownTick', ' BidTotalSizeUpTick',
              ' BidTotalSizeDownTick', 'BidTotalPriceUpTick', ' BidTotalPriceDownTick', ' BidPriceVolumeWeightedUpTick',
              ' BidPriceVolumeWeightedDownTick', ' BidFrequencyUpTick', ' BidFrequencyDownTick', ' BidMinPriceUpTick',
              'BidMinPriceDownTick', ' BidMaxPriceUpTick', ' BidMaxPriceDownTick', ' BidVolumePriceWeightedUpTick',
              ' BidVolumePriceWeightedDownTick', ' BidTickVolumeUpTick', ' BidTickVolumeDownTick', 'BidMinVolumeUpTick',
              ' BidMinVolumeDownTick', ' BidMaxVolumeUpTick', ' BidMaxVolumeDownTick', ' ValuePerBidUpTick',
              ' ValuePerBidDownTick', ' BidOpenPriceUpTick', ' BidClosePriceUpTick', 'BidOpenPriceDownTick',
              ' BidClosePriceDownTick', ' BidCloseVolumeUpTick', ' BidCloseVolumeDownTick', 'AskTotalValueUpTick',
              ' AskTotalValueDownTick', ' AskTotalSizeUpTick', 'AskTotalSizeDownTick', ' AskTotalPriceUpTick',
              ' AskTotalPriceDownTick', ' AskPriceVolumeWeightedUpTick', ' AskPriceVolumeWeightedDownTick',
              ' AskFrequencyUpTick', ' AskFrequencyDownTick', 'AskMinPriceUpTick', ' AskMinPriceDownTick',
              ' AskMaxPriceUpTick', ' AskMaxPriceDownTick', ' AskVolumePriceWeightedUpTick',
              ' AskVolumePriceWeightedDownTick', ' AskTickVolumeUpTick', 'AskTickVolumeDownTick', ' AskMinVolumeUpTick',
              ' AskMinVolumeDownTick', ' AskMaxVolumeUpTick', ' AskMaxVolumeDownTick', ' ValuePerAskUpTick',
              ' ValuePerAskDownTick', ' AskOpenPriceUpTick', 'AskClosePriceUpTick', ' AskOpenPriceDownTick',
              ' AskClosePriceDownTick', ' AskCloseVolumeUpTick', ' AskCloseVolumeDownTick', 'TradeUpStdDeviation',
              ' TradeDownStdDeviation', 'TradeUpMedian', 'TradeDownMedian', 'TradeUpProbability',
              'TradeDownProbability', 'ValuePerTradeCall', 'ValuePerTradePut', 'ValuePerBidCall', 'ValuePerBidPut',
              'ValuePerAskCall', 'ValuePerAskPut', 'TradeFrequencyUpTickCall', 'TradeFrequencyUpTickPut',
              'TradeFrequencyDownTickCall', 'TradeFrequencyDownTickPut', 'BidFrequencyUpTickCall',
              'BidFrequencyUpTickPut', 'BidFrequencyDownTickCall', 'BidFrequencyDownTickPut', 'AskFrequencyUpTickCall',
              'AskFrequencyUpTickPut', 'AskFrequencyDownTickCall', 'AskFrequencyDownTickPut', 'Strike', 'Bid Size',
              'Avg_Bid_Ask', 'Bid', 'Ask', 'Ask Size', 'High', 'Low', 'Last', 'Expiry', 'Volume', 'OpenInt',
              'Trade Size', 'DTE', 'Expiration', 'Current Underlying Price', 'LastVolume', 'LastOI', 'ivPCRask',
              'ivPCRbid', 'ivPCRtrade', 'ivcallask', 'ivcallbid', 'ivcalltrade', 'ivputask', 'ivputbid', 'ivputtrade',
              'diffVolumeCall', 'diffOiCall', 'diffVolumePut', 'diffOiPut']
    MatrixToCSVWritelmbdaOne(bucketout, MDPCR, Header, keyout, LastDateTime, CurrentDateTime, nos)


## Main function ends here and Sub functions start here
TradePrice = 0
BidPrice = 0
AskPrice = 0
TradeSize = 0
BidSize = 0
AskSize = 0
TradeVolume = 0
BidVolume = 0
AskVolume = 0
TradeUpPrice = [0]
TradeUpVolume = [0]
TradeDownPrice = [0]
TradeDownVolume = [0]
BidUpPrice = [0]
BidUpVolume = [0]
BidDownPrice = [0]
BidDownVolume = [0]
AskUpPrice = [0]
AskUpVolume = [0]
AskDownPrice = [0]
AskDownVolume = [0]
Median = 0
UpCutOff = 0.5
DownCutOff = 0.5
MedianUpCutOff = 0
MedianDownCutOff = 0
Open1 = 0
Close = 0
High = 0
Low = 0
Volume = 0
TradeFrequency = 0
TradeTotalPrice = 0
TradeTotalSize = 0
TradeTotalValue = 0
TradeMaxPrice = 0
TradeMinPrice = 0
TradeMaxVolume = 0
TradeMinVolume = 0
TradeTickPrice = 0
TradeTickVolume = 0
TradeOpenPriceUpTick = 0
TradeFrequencyUpTick = 0
TradeTotalPriceUpTick = 0
TradeTotalSizeUpTick = 0
TradeTotalValueUpTick = 0
TradeMaxPriceUpTick = 0
TradeMinPriceUpTick = 0
TradeMaxVolumeUpTick = 0
TradeMinVolumeUpTick = 0
TradeTickVolumeUpTick = 0
TradeOpenPriceDownTick = 0
TradeFrequencyDownTick = 0
TradeTotalPriceDownTick = 0
TradeTotalSizeDownTick = 0
TradeTotalValueDownTick = 0
TradeMaxPriceDownTick = 0
TradeMinPriceDownTick = 0
TradeMaxVolumeDownTick = 0
TradeMinVolumeDownTick = 0
TradeTickVolumeDownTick = 0
BidFrequency = 0
BidTotalPrice = 0
BidTotalSize = 0
BidTotalValue = 0
BidMaxPrice = 0
BidMinPrice = 0
BidMaxVolume = 0
BidMinVolume = 0
BidTickPrice = 0
BidTickVolume = 0
BidOpenPriceUpTick = 0
BidFrequencyUpTick = 0
BidTotalPriceUpTick = 0
BidTotalSizeUpTick = 0
BidTotalValueUpTick = 0
BidMaxPriceUpTick = 0
BidMinPriceUpTick = 0
BidMaxVolumeUpTick = 0
BidMinVolumeUpTick = 0
BidTickVolumeUpTick = 0
BidOpenPriceDownTick = 0
BidFrequencyDownTick = 0
BidTotalPriceDownTick = 0
BidTotalSizeDownTick = 0
BidTotalValueDownTick = 0
BidMaxPriceDownTick = 0
BidMinPriceDownTick = 0
BidMaxVolumeDownTick = 0
BidMinVolumeDownTick = 0
BidTickVolumeDownTick = 0
AskFrequency = 0
AskTotalPrice = 0
AskTotalSize = 0
AskTotalValue = 0
AskMaxPrice = 0
AskMinPrice = 0
AskMaxVolume = 0
AskMinVolume = 0
AskTickPrice = 0
AskTickVolume = 0
AskOpenPriceUpTick = 0
AskFrequencyUpTick = 0
AskTotalPriceUpTick = 0
AskTotalSizeUpTick = 0
AskTotalValueUpTick = 0
AskMaxPriceUpTick = 0
AskMinPriceUpTick = 0
AskMaxVolumeUpTick = 0
AskMinVolumeUpTick = 0
AskTickVolumeUpTick = 0
AskOpenPriceDownTick = 0
AskFrequencyDownTick = 0
AskTotalPriceDownTick = 0
AskTotalSizeDownTick = 0
AskTotalValueDownTick = 0
AskMaxPriceDownTick = 0
AskMinPriceDownTick = 0
AskMaxVolumeDownTick = 0
AskMinVolumeDownTick = 0
AskTickVolumeDownTick = 0
TradePriceVolumeWeighted = 0
TradeTotalPrice = 0
TradeCurrentPrice = 0
TradeMaxPrice = 0
TradeMinPrice = 0
TradeTickPrice = 0
TradeVolumePriceWeighted = 0
TradeTickVolume = 0
TradeMinVolume = 0
TradeMaxVolume = 0
TradeCurrentSize = 0
TradeTotalSize = 0
TradeTotalValue = 0
ValuePerTrade = 0
TradeFrequency = 0
BidCurrentPrice = 0
BidTotalPrice = 0
BidPriceVolumeWeighted = 0
BidMinPrice = 0
BidMaxPrice = 0
BidTickPrice = 0
BidVolumePriceWeighted = 0
BidTickVolume = 0
BidMinVolume = 0
BidMaxVolume = 0
BidCurrentSize = 0
BidTotalSize = 0
BidTotalValue = 0
ValuePerBid = 0
BidFrequency = 0
AskCurrentPrice = 0
AskTotalPrice = 0
AskPriceVolumeWeighted = 0
AskMinPrice = 0
AskMaxPrice = 0
AskTickPrice = 0
AskVolumePriceWeighted = 0
AskTickVolume = 0
AskMinVolume = 0
AskMaxVolume = 0
AskCurrentSize = 0
AskTotalSize = 0
AskTotalValue = 0
ValuePerAsk = 0
AskFrequency = 0
TradeTotalValueUpTick = 0
TradeTotalValueDownTick = 0
TradeTotalSizeUpTick = 0
TradeTotalSizeDownTick = 0
TradeTotalPriceUpTick = 0
TradeTotalPriceDownTick = 0
TradePriceVolumeWeightedUpTick = 0
TradePriceVolumeWeightedDownTick = 0
TradeFrequencyUpTick = 0
TradeFrequencyDownTick = 0
TradeMinPriceUpTick = 0
TradeMinPriceDownTick = 0
TradeMaxPriceUpTick = 0
TradeMaxPriceDownTick = 0
TradeVolumePriceWeightedUpTick = 0
TradeVolumePriceWeightedDownTick = 0
TradeTickVolumeUpTick = 0
TradeTickVolumeDownTick = 0
TradeMinVolumeUpTick = 0
TradeMinVolumeDownTick = 0
TradeMaxVolumeUpTick = 0
TradeMaxVolumeDownTick = 0
ValuePerTradeUpTick = 0
ValuePerTradeDownTick = 0
TradeOpenPriceUpTick = 0
TradeClosePriceUpTick = 0
TradeOpenPriceDownTick = 0
TradeClosePriceDownTick = 0
TradeCloseVolumeUpTick = 0
TradeCloseVolumeDownTick = 0
BidTotalValueUpTick = 0
BidTotalValueDownTick = 0
BidTotalSizeUpTick = 0
BidTotalSizeDownTick = 0
BidTotalPriceUpTick = 0
BidTotalPriceDownTick = 0
BidPriceVolumeWeightedUpTick = 0
BidPriceVolumeWeightedDownTick = 0
BidFrequencyUpTick = 0
BidFrequencyDownTick = 0
BidMinPriceUpTick = 0
BidMinPriceDownTick = 0
BidMaxPriceUpTick = 0
BidMaxPriceDownTick = 0
BidVolumePriceWeightedUpTick = 0
BidVolumePriceWeightedDownTick = 0
BidTickVolumeUpTick = 0
BidTickVolumeDownTick = 0
BidMinVolumeUpTick = 0
BidMinVolumeDownTick = 0
BidMaxVolumeUpTick = 0
BidMaxVolumeDownTick = 0
ValuePerBidUpTick = 0
ValuePerBidDownTick = 0
BidOpenPriceUpTick = 0
BidClosePriceUpTick = 0
BidOpenPriceDownTick = 0
BidClosePriceDownTick = 0
BidCloseVolumeUpTick = 0
BidCloseVolumeDownTick = 0
AskTotalValueUpTick = 0
AskTotalValueDownTick = 0
AskTotalSizeUpTick = 0
AskTotalSizeDownTick = 0
AskTotalPriceUpTick = 0
AskTotalPriceDownTick = 0
AskPriceVolumeWeightedUpTick = 0
AskPriceVolumeWeightedDownTick = 0
AskFrequencyUpTick = 0
AskFrequencyDownTick = 0
AskMinPriceUpTick = 0
AskMinPriceDownTick = 0
AskMaxPriceUpTick = 0
AskMaxPriceDownTick = 0
AskVolumePriceWeightedUpTick = 0
AskVolumePriceWeightedDownTick = 0
AskTickVolumeUpTick = 0
AskTickVolumeDownTick = 0
AskMinVolumeUpTick = 0
AskMinVolumeDownTick = 0
AskMaxVolumeUpTick = 0
AskMaxVolumeDownTick = 0
ValuePerAskUpTick = 0
ValuePerAskDownTick = 0
AskOpenPriceUpTick = 0
AskClosePriceUpTick = 0
AskOpenPriceDownTick = 0
AskClosePriceDownTick = 0
AskCloseVolumeUpTick = 0
AskCloseVolumeDownTick = 0
TradeUpStdDeviation = 0
TradeDownStdDeviation = 0
TradeUpMedian = 0
TradeDownMedian = 0
TradeUpProbability = 0
TradeDownProbability = 0
BidUpProbability = 0
BidDownProbability = 0
AskUpProbability = 0
AskDownProbability = 0


def InitToZero():
    global TradePrice
    global BidPrice
    global AskPrice
    global TradeSize
    global BidSize
    global AskSize

    global TradeUpPrice
    global TradeUpVolume
    global TradeDownPrice
    global TradeDownVolume
    global BidUpPrice
    global BidUpVolume
    global BidDownPrice
    global BidDownVolume
    global AskUpPrice
    global AskUpVolume
    global AskDownPrice
    global AskDownVolume

    global UpCutOff
    global DownCutOff
    global MedianUpCutOff
    global MedianDownCutOff
    global Open1
    global Close
    global High
    global Low
    global Volume
    global TradeFrequency
    global TradeTotalPrice
    global TradeTotalSize
    global TradeTotalValue
    global TradeMaxPrice
    global TradeMinPrice
    global TradeMaxVolume
    global TradeMinVolume
    global TradeTickPrice
    global TradeTickVolume
    global TradeOpenPriceUpTick
    global TradeFrequencyUpTick
    global TradeTotalPriceUpTick
    global TradeTotalSizeUpTick
    global TradeTotalValueUpTick
    global TradeMaxPriceUpTick
    global TradeMinPriceUpTick
    global TradeMaxVolumeUpTick
    global TradeMinVolumeUpTick
    global TradeTickVolumeUpTick
    global TradeOpenPriceDownTick
    global TradeFrequencyDownTick
    global TradeTotalPriceDownTick
    global TradeTotalSizeDownTick
    global TradeTotalValueDownTick
    global TradeMaxPriceDownTick
    global TradeMinPriceDownTick
    global TradeMaxVolumeDownTick
    global TradeMinVolumeDownTick
    global TradeTickVolumeDownTick
    global BidFrequency
    global BidTotalPrice
    global BidTotalSize
    global BidTotalValue
    global BidMaxPrice
    global BidMinPrice
    global BidMaxVolume
    global BidMinVolume
    global BidTickPrice
    global BidTickVolume
    global BidOpenPriceUpTick
    global BidFrequencyUpTick
    global BidTotalPriceUpTick
    global BidTotalSizeUpTick
    global BidTotalValueUpTick
    global BidMaxPriceUpTick
    global BidMinPriceUpTick
    global BidMaxVolumeUpTick
    global BidMinVolumeUpTick
    global BidTickVolumeUpTick
    global BidOpenPriceDownTick
    global BidFrequencyDownTick
    global BidTotalPriceDownTick
    global BidTotalSizeDownTick
    global BidTotalValueDownTick
    global BidMaxPriceDownTick
    global BidMinPriceDownTick
    global BidMaxVolumeDownTick
    global BidMinVolumeDownTick
    global BidTickVolumeDownTick
    global AskFrequency
    global AskTotalPrice
    global AskTotalSize
    global AskTotalValue
    global AskMaxPrice
    global AskMinPrice
    global AskMaxVolume
    global AskMinVolume
    global AskTickPrice
    global AskTickVolume
    global AskOpenPriceUpTick
    global AskFrequencyUpTick
    global AskTotalPriceUpTick
    global AskTotalSizeUpTick
    global AskTotalValueUpTick
    global AskMaxPriceUpTick
    global AskMinPriceUpTick
    global AskMaxVolumeUpTick
    global AskMinVolumeUpTick
    global AskTickVolumeUpTick
    global AskOpenPriceDownTick
    global AskFrequencyDownTick
    global AskTotalPriceDownTick
    global AskTotalSizeDownTick
    global AskTotalValueDownTick
    global AskMaxPriceDownTick
    global AskMinPriceDownTick
    global AskMaxVolumeDownTick
    global AskMinVolumeDownTick
    global AskTickVolumeDownTick
    global TradePriceVolumeWeighted
    global TradeTotalPrice
    global TradeCurrentPrice
    global TradeMaxPrice
    global TradeMinPrice
    global TradeTickPrice
    global TradeVolumePriceWeighted
    global TradeTickVolume
    global TradeMinVolume
    global TradeMaxVolume
    global TradeCurrentSize
    global TradeTotalSize
    global TradeTotalValue
    global ValuePerTrade
    global TradeFrequency
    global BidCurrentPrice
    global BidTotalPrice
    global BidPriceVolumeWeighted
    global BidMinPrice
    global BidMaxPrice
    global BidTickPrice
    global BidVolumePriceWeighted
    global BidTickVolume
    global BidMinVolume
    global BidMaxVolume
    global BidCurrentSize
    global BidTotalSize
    global BidTotalValue
    global ValuePerBid
    global BidFrequency
    global AskCurrentPrice
    global AskTotalPrice
    global AskPriceVolumeWeighted
    global AskMinPrice
    global AskMaxPrice
    global AskTickPrice
    global AskVolumePriceWeighted
    global AskTickVolume
    global AskMinVolume
    global AskMaxVolume
    global AskCurrentSize
    global AskTotalSize
    global AskTotalValue
    global ValuePerAsk
    global AskFrequency
    global TradeTotalValueUpTick
    global TradeTotalValueDownTick
    global TradeTotalSizeUpTick
    global TradeTotalSizeDownTick
    global TradeTotalPriceUpTick
    global TradeTotalPriceDownTick
    global TradePriceVolumeWeightedUpTick
    global TradePriceVolumeWeightedDownTick
    global TradeFrequencyUpTick
    global TradeFrequencyDownTick
    global TradeMinPriceUpTick
    global TradeMinPriceDownTick
    global TradeMaxPriceUpTick
    global TradeMaxPriceDownTick
    global TradeVolumePriceWeightedUpTick
    global TradeVolumePriceWeightedDownTick
    global TradeTickVolumeUpTick
    global TradeTickVolumeDownTick
    global TradeMinVolumeUpTick
    global TradeMinVolumeDownTick
    global TradeMaxVolumeUpTick
    global TradeMaxVolumeDownTick
    global ValuePerTradeUpTick
    global ValuePerTradeDownTick
    global TradeOpenPriceUpTick
    global TradeClosePriceUpTick
    global TradeOpenPriceDownTick
    global TradeClosePriceDownTick
    global TradeCloseVolumeUpTick
    global TradeCloseVolumeDownTick
    global BidTotalValueUpTick
    global BidTotalValueDownTick
    global BidTotalSizeUpTick
    global BidTotalSizeDownTick
    global BidTotalPriceUpTick
    global BidTotalPriceDownTick
    global BidPriceVolumeWeightedUpTick
    global BidPriceVolumeWeightedDownTick
    global BidFrequencyUpTick
    global BidFrequencyDownTick
    global BidMinPriceUpTick
    global BidMinPriceDownTick
    global BidMaxPriceUpTick
    global BidMaxPriceDownTick
    global BidVolumePriceWeightedUpTick
    global BidVolumePriceWeightedDownTick
    global BidTickVolumeUpTick
    global BidTickVolumeDownTick
    global BidMinVolumeUpTick
    global BidMinVolumeDownTick
    global BidMaxVolumeUpTick
    global BidMaxVolumeDownTick
    global ValuePerBidUpTick
    global ValuePerBidDownTick
    global BidOpenPriceUpTick
    global BidClosePriceUpTick
    global BidOpenPriceDownTick
    global BidClosePriceDownTick
    global BidCloseVolumeUpTick
    global BidCloseVolumeDownTick
    global AskTotalValueUpTick
    global AskTotalValueDownTick
    global AskTotalSizeUpTick
    global AskTotalSizeDownTick
    global AskTotalPriceUpTick
    global AskTotalPriceDownTick
    global AskPriceVolumeWeightedUpTick
    global AskPriceVolumeWeightedDownTick
    global AskFrequencyUpTick
    global AskFrequencyDownTick
    global AskMinPriceUpTick
    global AskMinPriceDownTick
    global AskMaxPriceUpTick
    global AskMaxPriceDownTick
    global AskVolumePriceWeightedUpTick
    global AskVolumePriceWeightedDownTick
    global AskTickVolumeUpTick
    global AskTickVolumeDownTick
    global AskMinVolumeUpTick
    global AskMinVolumeDownTick
    global AskMaxVolumeUpTick
    global AskMaxVolumeDownTick
    global ValuePerAskUpTick
    global ValuePerAskDownTick
    global AskOpenPriceUpTick
    global AskClosePriceUpTick
    global AskOpenPriceDownTick
    global AskClosePriceDownTick
    global AskCloseVolumeUpTick
    global AskCloseVolumeDownTick
    global TradeUpStdDeviation
    global TradeDownStdDeviation
    global TradeUpMedian
    global TradeDownMedian
    global TradeUpProbability
    global TradeDownProbability
    global BidUpProbability
    global BidDownProbability
    global AskUpProbability
    global AskDownProbability
    TradePrice = 0
    BidPrice = 0
    AskPrice = 0
    TradeSize = 0
    BidSize = 0
    AskSize = 0
    TradeVolume = 0
    BidVolume = 0
    AskVolume = 0

    TradeUpPrice = [0]
    TradeUpVolume = [0]
    TradeDownPrice = [0]
    TradeDownVolume = [0]
    BidUpPrice = [0]
    BidUpVolume = [0]
    BidDownPrice = [0]
    BidDownVolume = [0]
    AskUpPrice = [0]
    AskUpVolume = [0]
    AskDownPrice = [0]
    AskDownVolume = [0]

    UpCutOff = 0.5
    DownCutOff = 0.5
    MedianUpCutOff = 0
    MedianDownCutOff = 0
    Open1 = 0
    Close = 0
    High = 0
    Low = 0
    Volume = 0
    TradeFrequency = 0
    TradeTotalPrice = 0
    TradeTotalSize = 0
    TradeTotalValue = 0
    TradeMaxPrice = 0
    TradeMinPrice = 0
    TradeMaxVolume = 0
    TradeMinVolume = 0
    TradeTickPrice = 0
    TradeTickVolume = 0
    TradeOpenPriceUpTick = 0
    TradeFrequencyUpTick = 0
    TradeTotalPriceUpTick = 0
    TradeTotalSizeUpTick = 0
    TradeTotalValueUpTick = 0
    TradeMaxPriceUpTick = 0
    TradeMinPriceUpTick = 0
    TradeMaxVolumeUpTick = 0
    TradeMinVolumeUpTick = 0
    TradeTickVolumeUpTick = 0
    TradeOpenPriceDownTick = 0
    TradeFrequencyDownTick = 0
    TradeTotalPriceDownTick = 0
    TradeTotalSizeDownTick = 0
    TradeTotalValueDownTick = 0
    TradeMaxPriceDownTick = 0
    TradeMinPriceDownTick = 0
    TradeMaxVolumeDownTick = 0
    TradeMinVolumeDownTick = 0
    TradeTickVolumeDownTick = 0
    BidFrequency = 0
    BidTotalPrice = 0
    BidTotalSize = 0
    BidTotalValue = 0
    BidMaxPrice = 0
    BidMinPrice = 0
    BidMaxVolume = 0
    BidMinVolume = 0
    BidTickPrice = 0
    BidTickVolume = 0
    BidOpenPriceUpTick = 0
    BidFrequencyUpTick = 0
    BidTotalPriceUpTick = 0
    BidTotalSizeUpTick = 0
    BidTotalValueUpTick = 0
    BidMaxPriceUpTick = 0
    BidMinPriceUpTick = 0
    BidMaxVolumeUpTick = 0
    BidMinVolumeUpTick = 0
    BidTickVolumeUpTick = 0
    BidOpenPriceDownTick = 0
    BidFrequencyDownTick = 0
    BidTotalPriceDownTick = 0
    BidTotalSizeDownTick = 0
    BidTotalValueDownTick = 0
    BidMaxPriceDownTick = 0
    BidMinPriceDownTick = 0
    BidMaxVolumeDownTick = 0
    BidMinVolumeDownTick = 0
    BidTickVolumeDownTick = 0
    AskFrequency = 0
    AskTotalPrice = 0
    AskTotalSize = 0
    AskTotalValue = 0
    AskMaxPrice = 0
    AskMinPrice = 0
    AskMaxVolume = 0
    AskMinVolume = 0
    AskTickPrice = 0
    AskTickVolume = 0
    AskOpenPriceUpTick = 0
    AskFrequencyUpTick = 0
    AskTotalPriceUpTick = 0
    AskTotalSizeUpTick = 0
    AskTotalValueUpTick = 0
    AskMaxPriceUpTick = 0
    AskMinPriceUpTick = 0
    AskMaxVolumeUpTick = 0
    AskMinVolumeUpTick = 0
    AskTickVolumeUpTick = 0
    AskOpenPriceDownTick = 0
    AskFrequencyDownTick = 0
    AskTotalPriceDownTick = 0
    AskTotalSizeDownTick = 0
    AskTotalValueDownTick = 0
    AskMaxPriceDownTick = 0
    AskMinPriceDownTick = 0
    AskMaxVolumeDownTick = 0
    AskMinVolumeDownTick = 0
    AskTickVolumeDownTick = 0
    TradePriceVolumeWeighted = 0
    TradeTotalPrice = 0
    TradeCurrentPrice = 0
    TradeMaxPrice = 0
    TradeMinPrice = 0
    TradeTickPrice = 0
    TradeVolumePriceWeighted = 0
    TradeTickVolume = 0
    TradeMinVolume = 0
    TradeMaxVolume = 0
    TradeCurrentSize = 0
    TradeTotalSize = 0
    TradeTotalValue = 0
    ValuePerTrade = 0
    TradeFrequency = 0
    BidCurrentPrice = 0
    BidTotalPrice = 0
    BidPriceVolumeWeighted = 0
    BidMinPrice = 0
    BidMaxPrice = 0
    BidTickPrice = 0
    BidVolumePriceWeighted = 0
    BidTickVolume = 0
    BidMinVolume = 0
    BidMaxVolume = 0
    BidCurrentSize = 0
    BidTotalSize = 0
    BidTotalValue = 0
    ValuePerBid = 0
    BidFrequency = 0
    AskCurrentPrice = 0
    AskTotalPrice = 0
    AskPriceVolumeWeighted = 0
    AskMinPrice = 0
    AskMaxPrice = 0
    AskTickPrice = 0
    AskVolumePriceWeighted = 0
    AskTickVolume = 0
    AskMinVolume = 0
    AskMaxVolume = 0
    AskCurrentSize = 0
    AskTotalSize = 0
    AskTotalValue = 0
    ValuePerAsk = 0
    AskFrequency = 0
    TradeTotalValueUpTick = 0
    TradeTotalValueDownTick = 0
    TradeTotalSizeUpTick = 0
    TradeTotalSizeDownTick = 0
    TradeTotalPriceUpTick = 0
    TradeTotalPriceDownTick = 0
    TradePriceVolumeWeightedUpTick = 0
    TradePriceVolumeWeightedDownTick = 0
    TradeFrequencyUpTick = 0
    TradeFrequencyDownTick = 0
    TradeMinPriceUpTick = 0
    TradeMinPriceDownTick = 0
    TradeMaxPriceUpTick = 0
    TradeMaxPriceDownTick = 0
    TradeVolumePriceWeightedUpTick = 0
    TradeVolumePriceWeightedDownTick = 0
    TradeTickVolumeUpTick = 0
    TradeTickVolumeDownTick = 0
    TradeMinVolumeUpTick = 0
    TradeMinVolumeDownTick = 0
    TradeMaxVolumeUpTick = 0
    TradeMaxVolumeDownTick = 0
    ValuePerTradeUpTick = 0
    ValuePerTradeDownTick = 0
    TradeOpenPriceUpTick = 0
    TradeClosePriceUpTick = 0
    TradeOpenPriceDownTick = 0
    TradeClosePriceDownTick = 0
    TradeCloseVolumeUpTick = 0
    TradeCloseVolumeDownTick = 0
    BidTotalValueUpTick = 0
    BidTotalValueDownTick = 0
    BidTotalSizeUpTick = 0
    BidTotalSizeDownTick = 0
    BidTotalPriceUpTick = 0
    BidTotalPriceDownTick = 0
    BidPriceVolumeWeightedUpTick = 0
    BidPriceVolumeWeightedDownTick = 0
    BidFrequencyUpTick = 0
    BidFrequencyDownTick = 0
    BidMinPriceUpTick = 0
    BidMinPriceDownTick = 0
    BidMaxPriceUpTick = 0
    BidMaxPriceDownTick = 0
    BidVolumePriceWeightedUpTick = 0
    BidVolumePriceWeightedDownTick = 0
    BidTickVolumeUpTick = 0
    BidTickVolumeDownTick = 0
    BidMinVolumeUpTick = 0
    BidMinVolumeDownTick = 0
    BidMaxVolumeUpTick = 0
    BidMaxVolumeDownTick = 0
    ValuePerBidUpTick = 0
    ValuePerBidDownTick = 0
    BidOpenPriceUpTick = 0
    BidClosePriceUpTick = 0
    BidOpenPriceDownTick = 0
    BidClosePriceDownTick = 0
    BidCloseVolumeUpTick = 0
    BidCloseVolumeDownTick = 0
    AskTotalValueUpTick = 0
    AskTotalValueDownTick = 0
    AskTotalSizeUpTick = 0
    AskTotalSizeDownTick = 0
    AskTotalPriceUpTick = 0
    AskTotalPriceDownTick = 0
    AskPriceVolumeWeightedUpTick = 0
    AskPriceVolumeWeightedDownTick = 0
    AskFrequencyUpTick = 0
    AskFrequencyDownTick = 0
    AskMinPriceUpTick = 0
    AskMinPriceDownTick = 0
    AskMaxPriceUpTick = 0
    AskMaxPriceDownTick = 0
    AskVolumePriceWeightedUpTick = 0
    AskVolumePriceWeightedDownTick = 0
    AskTickVolumeUpTick = 0
    AskTickVolumeDownTick = 0
    AskMinVolumeUpTick = 0
    AskMinVolumeDownTick = 0
    AskMaxVolumeUpTick = 0
    AskMaxVolumeDownTick = 0
    ValuePerAskUpTick = 0
    ValuePerAskDownTick = 0
    AskOpenPriceUpTick = 0
    AskClosePriceUpTick = 0
    AskOpenPriceDownTick = 0
    AskClosePriceDownTick = 0
    AskCloseVolumeUpTick = 0
    AskCloseVolumeDownTick = 0
    TradeUpStdDeviation = 0
    TradeDownStdDeviation = 0
    TradeUpMedian = 0
    TradeDownMedian = 0
    TradeUpProbability = 0
    TradeDownProbability = 0
    BidUpProbability = 0
    BidDownProbability = 0
    AskUpProbability = 0
    AskDownProbability = 0


def CSVtoPRMTR(key):
    # print('PRMTR Key :', key)
    data = list(csv.reader(open(key)))
    # print (data)
    return data


def IvOiCSVtoArray(key, stp):
    stpkey = 'Y:\\vulturespicks3tokyo\\Controllers\\STPKeys.csv'
    MatrixSTP = list(csv.reader(open(stpkey)))
    nRows = len(MatrixSTP)
    nColumns = len(MatrixSTP[0])
    rowindex = 0
    columnindex = 0
    for x in range(1, nRows):
        for y in range(0, nColumns):
            if (stp == str(MatrixSTP[x][y])):
                rowindex = x
                columnindex = y
    # print('rowindex', rowindex)
    data = list(csv.reader(open(key)))
    nRows = len(data)
    nColumns = len(data[0])
    Matrix = [[0 for x in range(nColumns)] for y in range(nRows)]
    for x in range(1, nRows):
        if (x == rowindex):
            for y in range(5, nColumns):
                try:
                    lpart = float(data[x][y])
                except:
                    lpart = 1  # set date as 1 as I dont need it
                Matrix[x][y] = lpart
    # print('Matrix', Matrix)
    rMatrix = [row[5:nColumns] for row in Matrix[rowindex:rowindex + 1]]
    return rMatrix[0]


def erfcc(x):
    """Complementary error function."""
    z = abs(x)
    t = 1. / (1. + 0.5 * z)
    r = t * exp(-z * z - 1.26551223 + t * (1.00002368 + t * (.37409196 +
                                                             t * (.09678418 + t * (-.18628806 + t * (.27886807 +
                                                                                                     t * (
                                                                                                     -1.13520398 + t * (
                                                                                                     1.48851587 + t * (
                                                                                                     -.82215223 +
                                                                                                     t * .17087277)))))))))
    if (x >= 0.):
        return r
    else:
        return 2. - r


def ncdf(x):
    return 1. - 0.5 * erfcc(x / (2 ** 0.5))


def implied_volatility(Matrix, optiontype, askbidtrade):
    if (askbidtrade == 'ask'): P = float(Matrix[4])
    if (askbidtrade == 'bid'): P = float(Matrix[3])
    if (askbidtrade == 'trade'): P = float(Matrix[8])
    S = float(Matrix[15])
    E = float(Matrix[0])
    T = float(Matrix[13]) / 365
    r = float(0.12)
    dVol = 0.00001
    epsilon = 0.00001
    maxIter = 50
    sigma = 0.2
    # print (P, S, E, T, r)
    try:
        i = 1
        while i < maxIter:

            d_1 = float(float((math.log(S / E) + (r + (sigma ** 2) / 2) * T)) / float((sigma * (math.sqrt(T)))))
            d_2 = float(float((math.log(S / E) + (r - (sigma ** 2) / 2) * T)) / float((sigma * (math.sqrt(T)))))
            # if (optiontype == 'Call'):P_implied = float(S*norm.cdf(d_1) - E*math.exp(-r*T)*norm.cdf(d_2))# for call
            # if (optiontype == 'Put'): P_implied = float(E*math.exp(-r*T)*norm.cdf(-d_2) - S*norm.cdf(-d_1)) # for put
            if (optiontype == 'Call'): P_implied = float(S * ncdf(d_1) - E * math.exp(-r * T) * ncdf(d_2))  # for call
            if (optiontype == 'Put'): P_implied = float(E * math.exp(-r * T) * ncdf(-d_2) - S * ncdf(-d_1))  # for put
            sigma = sigma - dVol
            sigma1 = sigma
            d_1 = float(float((math.log(S / E) + (r + (sigma ** 2) / 2) * T)) / float((sigma * (math.sqrt(T)))))
            d_2 = float(float((math.log(S / E) + (r - (sigma ** 2) / 2) * T)) / float((sigma * (math.sqrt(T)))))
            # if (optiontype == 'Call'): P_impliedT = float(S*norm.cdf(d_1) - E*math.exp(-r*T)*norm.cdf(d_2)) #for call
            # if (optiontype == 'Put'):  P_impliedT = float(E*math.exp(-r*T)*norm.cdf(-d_2) - S*norm.cdf(-d_1)) # for put
            if (optiontype == 'Call'): P_impliedT = float(S * ncdf(d_1) - E * math.exp(-r * T) * ncdf(d_2))  # for call
            if (optiontype == 'Put'):  P_impliedT = float(
                E * math.exp(-r * T) * ncdf(-d_2) - S * ncdf(-d_1))  # for put
            dx = (P_impliedT - P_implied) / dVol
            if (math.fabs(dx) < epsilon or i == maxIter):
                break
            sigma = sigma1 - (P - P_implied) / dx
            if (sigma < 0.05): sigma = 0.05
            if (sigma > 0.95): sigma = 0.95
            i = i + 1
            # print('P, P_impliedT',P, P_impliedT )
            # return sigma
    except Exception as e:
        sigma = 0.2
    return sigma


def TickCSVtoMatrix(key, LastDateTime):
    data = list(csv.reader(open(key)))
    nRows = len(data)
    nColumns = len(data[0])
    # print("newBodynRows: ", nRows)
    # print("newBodynColumns: ", nColumns)
    dcontrol = 0
    selectfrom = 0
    # print("nRows: ", nRows)
    Matrix = [[0 for x in range(nColumns)] for y in range(nRows)]
    for x in range(1, nRows):
        dte = data[x][0]
        tme = data[x][1]
        dtetme = dte + ' ' + tme
        dtetme = datetime.datetime.strptime(dtetme, '%m/%d/%Y %H:%M:%S')
        if (dtetme > LastDateTime):
            if (dcontrol == 0):
                dcontrol = 1
                selectfrom = x
            for y in range(2, nColumns):
                Matrix[x][y] = data[x][y]
    rMatrix = [row[2:8] for row in Matrix[selectfrom:x]]
    # print('rMatrix', rMatrix)
    return rMatrix


def computeHigh(current, high):
    if high < current or high == 0:
        high = current
    return high


def computeLow(current, low):
    if low > current or low == 0:
        low = current
    return low


def marketdynamic(data):
    global TradePrice
    global BidPrice
    global AskPrice
    global TradeSize
    global BidSize
    global AskSize
    global Open1
    global Close
    global High
    global Low
    global Volume
    try:
        nRows = len(data)
        # print('Data', data)
        nColumns = len(data[0])

        emptystr = ''
        for i in range(0, nRows):
            # print('Row of Data:', i)
            TradePrice = 0
            BidPrice = 0
            AskPrice = 0
            TradeSize = 0
            BidSize = 0
            AskSize = 0

            if (str(data[i][0]) == emptystr):
                tmp = 0
            else:
                # print('data', data[i][0] )
                TradePrice = float(data[i][0])
                TradeSize = float(data[i][1])
            if (str(data[i][2]) == emptystr):
                tmp = 0
            else:
                BidPrice = float(data[i][2])
                BidSize = float(data[i][3])
            if (str(data[i][2]) == emptystr):
                tmp = 0
            else:
                AskPrice = float(data[i][4])
                AskSize = float(data[i][5])
            TradeCalculation()
            BidCalculation()
            AskCalculation()

        TradeUpCalculation()
        TradeDownCalculation()
        BidUpCalculation()
        BidDownCalculation()
        AskUpCalculation()
        AskDownCalculation()
        Close = TradeCurrentPrice
        High = TradeMaxPrice
        Low = TradeMinPrice
        Volume = TradeVolumePriceWeighted
        mdarray = [Open1, TradePriceVolumeWeighted, TradeTotalPrice, Close, High, Low, TradeTickPrice, Volume,
                   TradeTickVolume, TradeMinVolume, TradeMaxVolume, TradeCurrentSize, TradeTotalSize, TradeTotalValue,
                   ValuePerTrade, TradeFrequency, BidCurrentPrice, BidTotalPrice, BidPriceVolumeWeighted, BidMinPrice,
                   BidMaxPrice, BidTickPrice, BidVolumePriceWeighted, BidTickVolume, BidMinVolume, BidMaxVolume,
                   BidCurrentSize, BidTotalSize, BidTotalValue, ValuePerBid, BidFrequency, AskCurrentPrice,
                   AskTotalPrice, AskPriceVolumeWeighted, AskMinPrice, AskMaxPrice, AskTickPrice,
                   AskVolumePriceWeighted, AskTickVolume, AskMinVolume, AskMaxVolume, AskCurrentSize, AskTotalSize,
                   AskTotalValue, ValuePerAsk, AskFrequency, TradeTotalValueUpTick, TradeTotalValueDownTick,
                   TradeTotalSizeUpTick, TradeTotalSizeDownTick, TradeTotalPriceUpTick, TradeTotalPriceDownTick,
                   TradePriceVolumeWeightedUpTick, TradePriceVolumeWeightedDownTick, TradeFrequencyUpTick,
                   TradeFrequencyDownTick, TradeMinPriceUpTick, TradeMinPriceDownTick, TradeMaxPriceUpTick,
                   TradeMaxPriceDownTick, TradeVolumePriceWeightedUpTick, TradeVolumePriceWeightedDownTick,
                   TradeTickVolumeUpTick, TradeTickVolumeDownTick, TradeMinVolumeUpTick, TradeMinVolumeDownTick,
                   TradeMaxVolumeUpTick, TradeMaxVolumeDownTick, ValuePerTradeUpTick, ValuePerTradeDownTick,
                   TradeOpenPriceUpTick, TradeClosePriceUpTick, TradeOpenPriceDownTick, TradeClosePriceDownTick,
                   TradeCloseVolumeUpTick, TradeCloseVolumeDownTick, BidTotalValueUpTick, BidTotalValueDownTick,
                   BidTotalSizeUpTick, BidTotalSizeDownTick, BidTotalPriceUpTick, BidTotalPriceDownTick,
                   BidPriceVolumeWeightedUpTick, BidPriceVolumeWeightedDownTick, BidFrequencyUpTick,
                   BidFrequencyDownTick, BidMinPriceUpTick, BidMinPriceDownTick, BidMaxPriceUpTick, BidMaxPriceDownTick,
                   BidVolumePriceWeightedUpTick, BidVolumePriceWeightedDownTick, BidTickVolumeUpTick,
                   BidTickVolumeDownTick, BidMinVolumeUpTick, BidMinVolumeDownTick, BidMaxVolumeUpTick,
                   BidMaxVolumeDownTick, ValuePerBidUpTick, ValuePerBidDownTick, BidOpenPriceUpTick,
                   BidClosePriceUpTick, BidOpenPriceDownTick, BidClosePriceDownTick, BidCloseVolumeUpTick,
                   BidCloseVolumeDownTick, AskTotalValueUpTick, AskTotalValueDownTick, AskTotalSizeUpTick,
                   AskTotalSizeDownTick, AskTotalPriceUpTick, AskTotalPriceDownTick, AskPriceVolumeWeightedUpTick,
                   AskPriceVolumeWeightedDownTick, AskFrequencyUpTick, AskFrequencyDownTick, AskMinPriceUpTick,
                   AskMinPriceDownTick, AskMaxPriceUpTick, AskMaxPriceDownTick, AskVolumePriceWeightedUpTick,
                   AskVolumePriceWeightedDownTick, AskTickVolumeUpTick, AskTickVolumeDownTick, AskMinVolumeUpTick,
                   AskMinVolumeDownTick, AskMaxVolumeUpTick, AskMaxVolumeDownTick, ValuePerAskUpTick,
                   ValuePerAskDownTick, AskOpenPriceUpTick, AskClosePriceUpTick, AskOpenPriceDownTick,
                   AskClosePriceDownTick, AskCloseVolumeUpTick, AskCloseVolumeDownTick, TradeUpStdDeviation,
                   TradeDownStdDeviation, TradeUpMedian, TradeDownMedian, TradeUpProbability, TradeDownProbability]
        return mdarray
    except Exception as e:
        mdarray = [Open1, TradePriceVolumeWeighted, TradeTotalPrice, Close, High, Low, TradeTickPrice, Volume,
                   TradeTickVolume, TradeMinVolume, TradeMaxVolume, TradeCurrentSize, TradeTotalSize, TradeTotalValue,
                   ValuePerTrade, TradeFrequency, BidCurrentPrice, BidTotalPrice, BidPriceVolumeWeighted, BidMinPrice,
                   BidMaxPrice, BidTickPrice, BidVolumePriceWeighted, BidTickVolume, BidMinVolume, BidMaxVolume,
                   BidCurrentSize, BidTotalSize, BidTotalValue, ValuePerBid, BidFrequency, AskCurrentPrice,
                   AskTotalPrice, AskPriceVolumeWeighted, AskMinPrice, AskMaxPrice, AskTickPrice,
                   AskVolumePriceWeighted, AskTickVolume, AskMinVolume, AskMaxVolume, AskCurrentSize, AskTotalSize,
                   AskTotalValue, ValuePerAsk, AskFrequency, TradeTotalValueUpTick, TradeTotalValueDownTick,
                   TradeTotalSizeUpTick, TradeTotalSizeDownTick, TradeTotalPriceUpTick, TradeTotalPriceDownTick,
                   TradePriceVolumeWeightedUpTick, TradePriceVolumeWeightedDownTick, TradeFrequencyUpTick,
                   TradeFrequencyDownTick, TradeMinPriceUpTick, TradeMinPriceDownTick, TradeMaxPriceUpTick,
                   TradeMaxPriceDownTick, TradeVolumePriceWeightedUpTick, TradeVolumePriceWeightedDownTick,
                   TradeTickVolumeUpTick, TradeTickVolumeDownTick, TradeMinVolumeUpTick, TradeMinVolumeDownTick,
                   TradeMaxVolumeUpTick, TradeMaxVolumeDownTick, ValuePerTradeUpTick, ValuePerTradeDownTick,
                   TradeOpenPriceUpTick, TradeClosePriceUpTick, TradeOpenPriceDownTick, TradeClosePriceDownTick,
                   TradeCloseVolumeUpTick, TradeCloseVolumeDownTick, BidTotalValueUpTick, BidTotalValueDownTick,
                   BidTotalSizeUpTick, BidTotalSizeDownTick, BidTotalPriceUpTick, BidTotalPriceDownTick,
                   BidPriceVolumeWeightedUpTick, BidPriceVolumeWeightedDownTick, BidFrequencyUpTick,
                   BidFrequencyDownTick, BidMinPriceUpTick, BidMinPriceDownTick, BidMaxPriceUpTick, BidMaxPriceDownTick,
                   BidVolumePriceWeightedUpTick, BidVolumePriceWeightedDownTick, BidTickVolumeUpTick,
                   BidTickVolumeDownTick, BidMinVolumeUpTick, BidMinVolumeDownTick, BidMaxVolumeUpTick,
                   BidMaxVolumeDownTick, ValuePerBidUpTick, ValuePerBidDownTick, BidOpenPriceUpTick,
                   BidClosePriceUpTick, BidOpenPriceDownTick, BidClosePriceDownTick, BidCloseVolumeUpTick,
                   BidCloseVolumeDownTick, AskTotalValueUpTick, AskTotalValueDownTick, AskTotalSizeUpTick,
                   AskTotalSizeDownTick, AskTotalPriceUpTick, AskTotalPriceDownTick, AskPriceVolumeWeightedUpTick,
                   AskPriceVolumeWeightedDownTick, AskFrequencyUpTick, AskFrequencyDownTick, AskMinPriceUpTick,
                   AskMinPriceDownTick, AskMaxPriceUpTick, AskMaxPriceDownTick, AskVolumePriceWeightedUpTick,
                   AskVolumePriceWeightedDownTick, AskTickVolumeUpTick, AskTickVolumeDownTick, AskMinVolumeUpTick,
                   AskMinVolumeDownTick, AskMaxVolumeUpTick, AskMaxVolumeDownTick, ValuePerAskUpTick,
                   ValuePerAskDownTick, AskOpenPriceUpTick, AskClosePriceUpTick, AskOpenPriceDownTick,
                   AskClosePriceDownTick, AskCloseVolumeUpTick, AskCloseVolumeDownTick, TradeUpStdDeviation,
                   TradeDownStdDeviation, TradeUpMedian, TradeDownMedian, TradeUpProbability, TradeDownProbability]
        return mdarray


def TradeCalculation():
    global TradePrice
    global TradeSize
    global Open1
    global AskCurrentPrice
    global BidCurrentPrice
    global TradeCurrentPrice
    global TradeTickPrice
    global AskBidMidPrice
    global TradeUpProbability
    global TradeUpPrice
    global TradeUpVolume
    global TradeDownProbability
    global TradeDownPrice
    global TradeDownVolume
    global TradeFrequency
    global TradeMinPrice
    global TradeMaxPrice
    global TradeTickVolume
    global TradeCurrentSize
    global TradeTotalValue
    global TradeTotalSize
    global TradeTotalPrice
    global TradePriceVolumeWeighted
    global TradeVolumePriceWeighted
    global TradeMinVolume
    global TradeMaxVolume
    global ValuePerTrade
    Up = True

    if TradeSize > 0:
        TradeVolume = TradeSize
    if TradePrice > 0:
        TradePrice = TradePrice
        AskBidMidPrice = (AskCurrentPrice + BidCurrentPrice) / 2
        if Open1 == 0:
            Open1 = TradePrice
        # region "TradeTickPrice"
        if TradeCurrentPrice >= 0:
            if TradePrice > TradeCurrentPrice:
                TradeTickPrice = TradeTickPrice + 1
                # 'TradePriceCalculationUpTick ''//beacause of Mean and Deviation
                if AskBidMidPrice > 0:
                    TradeUpProbability = TradeUpProbability + (TradePrice / AskBidMidPrice)
                TradeUpPrice.append((TradePrice))
                TradeUpVolume.append((TradeVolume))

                Up = True
            elif TradePrice < TradeCurrentPrice:
                TradeTickPrice = TradeTickPrice - 1
                # 'TradePriceCalculationDownTick  ''(, TradePrice, TradeVolume)
                TradeDownProbability = TradeDownProbability + (AskBidMidPrice / TradePrice)
                TradeDownPrice.append((TradePrice))
                TradeDownVolume.append((TradeVolume))

                Up = False
            elif Up == True:
                # 'TradePriceCalculationUpTick  ''(, TradePrice, TradeVolume)
                if AskBidMidPrice > 0:
                    TradeUpProbability = TradeUpProbability + (TradePrice / AskBidMidPrice)
                TradeUpPrice.append((TradePrice))
                TradeUpVolume.append((TradeVolume))

                Up = True
            else:
                # 'TradePriceCalculationDownTick  ''(, TradePrice, TradeVolume)
                TradeDownProbability = TradeDownProbability + (AskBidMidPrice / TradePrice)
                TradeDownPrice.append((TradePrice))
                TradeDownVolume.append((TradeVolume))

                Up = False
        ##End Region
        TradeCurrentPrice = TradePrice
        TradeFrequency = TradeFrequency + 1

        if TradeMinPrice > TradeCurrentPrice or TradeMinPrice == 0:
            TradeMinPrice = TradeCurrentPrice
        if TradeMaxPrice < TradeCurrentPrice or TradeMaxPrice == 0:
            TradeMaxPrice = TradeCurrentPrice
        if TradeSize >= 0:

            if TradeCurrentSize > 0:
                if TradeVolume > TradeCurrentSize:
                    TradeTickVolume = TradeTickVolume + 1
                elif TradeVolume < TradeCurrentSize:
                    TradeTickVolume = TradeTickVolume - 1
            ##End Region
            TradeCurrentSize = TradeVolume
            # Total Value and Total Price will reinitialize by 0 after Interval change
            TradeTotalValue = TradeTotalValue + TradeCurrentPrice * TradeCurrentSize
            TradeTotalSize = TradeTotalSize + TradeCurrentSize
            TradeTotalPrice = TradeTotalPrice + TradeCurrentPrice
            # TradePriceVolumeWeighted WILL CALCULATE AFTER ENTERVAL END
            if TradeTotalSize > 0:
                TradePriceVolumeWeighted = TradeTotalValue / TradeTotalSize
            if TradeTotalPrice > 0:
                TradeVolumePriceWeighted = TradeTotalValue / TradeTotalPrice
            if TradeMinVolume > TradeCurrentSize or TradeMinVolume == 0:
                TradeMinVolume = TradeCurrentSize
            # 'TradeMinVolume = computeLow(TradeCurrentSize, TradeMinVolume)
            # 'TradeMaxVolume = computeHigh(TradeCurrentSize, TradeMaxVolume)
            if TradeMaxVolume < TradeCurrentSize or TradeMaxVolume == 0:
                TradeMaxVolume = TradeCurrentSize
            if TradeFrequency > 0:
                ValuePerTrade = TradeTotalValue / TradeFrequency
            else:
                ValuePerTrade = 0


def BidCalculation():
    global BidPrice
    global BidSize
    global BidCurrentPrice
    global BidTickPrice
    global AskBidMidPrice
    global BidUpProbability
    global BidUpPrice
    global BidUpVolume
    global BidDownProbability
    global BidDownPrice
    global BidDownVolume
    global BidFrequency
    global BidMinPrice
    global BidMaxPrice
    global BidTickVolume
    global BidCurrentSize
    global BidTotalValue
    global BidTotalSize
    global BidTotalPrice
    global BidPriceVolumeWeighted
    global BidVolumePriceWeighted
    global BidMinVolume
    global BidMaxVolume
    global ValuePerBid
    Up = True

    if BidSize > 0:
        BidVolume = BidSize
    if BidPrice > 0:
        BidPrice = BidPrice
        # 'AskBidMidPrice = (AskCurrentPrice + BidCurrentPrice) / 2
        # ''#region "BidTickPrice"
        if BidCurrentPrice >= 0:
            if BidPrice > BidCurrentPrice:
                BidTickPrice = BidTickPrice + 1
                # 'BidPriceCalculationUpTick ''//beacause of Mean and Deviation
                BidUpPrice.append((BidPrice))
                BidUpVolume.append((BidVolume))
                Up = True
            elif BidPrice < BidCurrentPrice:
                BidTickPrice = BidTickPrice - 1
                # 'BidPriceCalculationDownTick  ''(, BidPrice, BidVolume)
                BidDownPrice.append((BidPrice))
                BidDownVolume.append((BidVolume))
                Up = False
            elif Up == True:
                # 'BidPriceCalculationUpTick  ''(, BidPrice, BidVolume)
                BidUpPrice.append((BidPrice))
                BidUpVolume.append((BidVolume))
                Up = True
            else:
                # 'BidPriceCalculationDownTick  ''(, BidPrice, BidVolume)
                BidDownPrice.append((BidPrice))
                BidDownVolume.append((BidVolume))
                Up = False
        ##End Region
        BidCurrentPrice = BidPrice
        BidFrequency = BidFrequency + 1
        BidMinPrice = computeLow(BidCurrentPrice, BidMinPrice)
        BidMaxPrice = computeHigh(BidCurrentPrice, BidMaxPrice)
        if BidSize >= 0:
            # ..SplittedValues[3] = Convert.ToString(Convert.ToDouble(..SplittedValues[3]) / EnumerationAndConstant.ValueScalarForVolume)
            # ''#region "TdadeTickVolume"
            if BidCurrentSize > 0:
                if BidVolume > BidCurrentSize:
                    BidTickVolume = BidTickVolume + 1
                elif BidVolume < BidCurrentSize:
                    BidTickVolume = BidTickVolume - 1
            ##End Region
            BidCurrentSize = BidVolume
            # Total Value and Total Price will reinitialize by 0 after Interval change
            BidTotalValue = BidTotalValue + (BidCurrentPrice * BidCurrentSize)
            BidTotalSize = BidTotalSize + BidCurrentSize
            BidTotalPrice = BidTotalPrice + BidCurrentPrice
            # BidPriceVolumeWeighted WILL CALCULATE AFTER ENTERVAL END
            if BidTotalSize > 0:
                BidPriceVolumeWeighted = BidTotalValue / BidTotalSize
            if BidTotalPrice > 0:
                BidVolumePriceWeighted = BidTotalValue / BidTotalPrice
            BidMinVolume = computeLow(BidCurrentSize, BidMinVolume)
            BidMaxVolume = computeHigh(BidCurrentSize, BidMaxVolume)
            if BidFrequency > 0:
                ValuePerBid = BidTotalValue / BidFrequency
            else:
                ValuePerBid = 0


def AskCalculation():
    global AskPrice
    global AskSize
    global AskCurrentPrice
    global AskTickPrice
    global AskAskMidPrice
    global AskUpProbability
    global AskUpPrice
    global AskUpVolume
    global AskDownProbability
    global AskDownPrice
    global AskDownVolume
    global AskFrequency
    global AskMinPrice
    global AskMaxPrice
    global AskTickVolume
    global AskCurrentSize
    global AskTotalValue
    global AskTotalSize
    global AskTotalPrice
    global AskPriceVolumeWeighted
    global AskVolumePriceWeighted
    global AskMinVolume
    global AskMaxVolume
    global ValuePerAsk
    Up = True

    if AskSize > 0:
        AskVolume = AskSize
    if AskPrice > 0:
        AskPrice = AskPrice
        # 'AskAskMidPrice = (AskCurrentPrice + AskCurrentPrice) / 2
        # ''#region "AskTickPrice"
        if AskCurrentPrice >= 0:
            if AskPrice > AskCurrentPrice:
                AskTickPrice = AskTickPrice + 1
                # 'AskPriceCalculationUpTick ''//beacause of Mean and Deviation
                AskUpPrice.append((AskPrice))
                AskUpVolume.append((AskVolume))
                Up = True
            elif AskPrice < AskCurrentPrice:
                AskTickPrice = AskTickPrice - 1
                # 'AskPriceCalculationDownTick  ''(, AskPrice, AskVolume)
                AskDownPrice.append((AskPrice))
                AskDownVolume.append((AskVolume))
                Up = False
            elif Up == True:
                # 'AskPriceCalculationUpTick  ''(, AskPrice, AskVolume)
                AskUpPrice.append((AskPrice))
                AskUpVolume.append((AskVolume))
                Up = True
            else:
                # 'AskPriceCalculationDownTick  ''(, AskPrice, AskVolume)
                AskDownPrice.append((AskPrice))
                AskDownVolume.append((AskVolume))
                Up = False
        ##End Region
        AskCurrentPrice = AskPrice
        AskFrequency = AskFrequency + 1
        AskMinPrice = computeLow(AskCurrentPrice, AskMinPrice)
        AskMaxPrice = computeHigh(AskCurrentPrice, AskMaxPrice)
        if AskSize >= 0:
            # ..SplittedValues[3] = Convert.ToString(Convert.ToDouble(..SplittedValues[3]) / EnumerationAndConstant.ValueScalarForVolume)
            # ''#region "TdadeTickVolume"
            if AskCurrentSize > 0:
                if AskVolume > AskCurrentSize:
                    AskTickVolume = AskTickVolume + 1
                elif AskVolume < AskCurrentSize:
                    AskTickVolume = AskTickVolume - 1
            ##End Region
            AskCurrentSize = AskVolume
            # Total Value and Total Price will reinitialize by 0 after Interval change
            AskTotalValue = AskTotalValue + (AskCurrentPrice * AskCurrentSize)
            AskTotalSize = AskTotalSize + AskCurrentSize
            AskTotalPrice = AskTotalPrice + AskCurrentPrice
            # AskPriceVolumeWeighted WILL CALCULATE AFTER ENTERVAL END
            if AskTotalSize > 0:
                AskPriceVolumeWeighted = AskTotalValue / AskTotalSize
            if AskTotalPrice > 0:
                AskVolumePriceWeighted = AskTotalValue / AskTotalPrice
            AskMinVolume = computeLow(AskCurrentSize, AskMinVolume)
            AskMaxVolume = computeHigh(AskCurrentSize, AskMaxVolume)
            if AskFrequency > 0:
                ValuePerAsk = AskTotalValue / AskFrequency
            else:
                ValuePerAsk = 0

                ##TRADE Up & Down Calculation Starts


def BubbleSort(TempArray):
    NoExchanges = 'True'
    # Loop until no more "exchanges" are made.
    while NoExchanges == 'True':
        # Loop through each element in the array.
        for i in range(1, len(TempArray) - 1):
            # If the element is greater than the element
            # following it, exchange the two elements.
            if TempArray[i] > TempArray[i + 1]:
                NoExchanges = False
                Temp = TempArray[i]
                TempArray[i] = TempArray[i + 1]
                TempArray[i + 1] = Temp
        if not (not (NoExchanges)):
            break
    return TempArray


def CalculateStandardDeviation(Data_Series):
    Total_numbers = len(Data_Series)
    Total_Variance = 0
    if Total_numbers == 0:
        Standard_Deviation = 0
    else:
        # Array.Sort(Data_Series);
        Med = CalculationForMedian(Data_Series)
        for i in range(0, Total_numbers - 1):
            Total_Variance = Total_Variance + ((Data_Series[i] - Med) * (Data_Series[i] - Med))
        Standard_Deviation = (math.sqrt((Total_Variance / Total_numbers)))
    return Standard_Deviation


def CalculationForMedian(Data_Series):
    Length = len(Data_Series)
    # print(Length)
    # global Median
    Series = [0 for y in range(Length)]
    # 'ReDim Series(10000)
    for i in range(0, Length - 1):
        Series[i] = Data_Series[i]
        # Series = BubbleSort(Series)
    Series.sort
    # print('series', Series)
    mod2 = math.fmod(Length, 2)
    if Length == 0: Med = 0
    if Length == 1: Med = Series[0]
    len2 = int(Length / 2)
    len2m1 = int(len2 - 1)
    if mod2 == 0:
        Med = Series[len2] + Series[len2m1]
        Med = Med / 2
    if mod2 == 1: Med = Series[len2]
    return Med


def TradeUpCalculation():
    global TradeOpenPriceUpTick
    global TradeClosePriceUpTick
    global TradeCloseVolumeUpTick
    global TradeUpStdDeviation
    global TradeUpProbability
    global TradeUpMedian
    if len(TradeUpVolume) > 0 and len(TradeUpPrice) > 0:
        # 'TradeUpVolume_str = TradeUpVolume.Clone
        TotalLength = len(TradeUpVolume)

        TradeUpValue = [0 for x in range(TotalLength)]

        for i in range(0, TotalLength - 1):
            TradeUpValue[i] = TradeUpPrice[i] * TradeUpVolume[i]
        Median = CalculationForMedian(TradeUpValue)

        # Median=np.median(TradeUpValue)
        Deviation = CalculateStandardDeviation(TradeUpValue)
        # Deviation=np.std(TradeUpValue)
        UpCut = int(((MedianUpCutOff * TotalLength) / 100))
        DownCut = int(((MedianDownCutOff * TotalLength) / 100))

        TradeUpValue.sort
        TradeOpenPriceUpTick = TradeUpPrice[0]
        for i in range(0, TotalLength - 1):
            TradePrice = TradeUpPrice[i]
            TradeVolume = TradeUpVolume[i]
            TradeValue = TradePrice * TradeVolume
            for j in range(DownCut, TotalLength - UpCut - 1):
                if TradeValue == TradeUpValue[j]:
                    TradePriceCalculationUpTick()
                    break
        TradeClosePriceUpTick = TradeUpPrice[TotalLength - 1]
        TradeCloseVolumeUpTick = TradeUpVolume[TotalLength - 1]
        TradeUpStdDeviation = Deviation
        TradeUpMedian = Median
        if TotalLength > 0:
            TradeUpProbability = TradeUpProbability / TotalLength
        TradePriceVolumeCalculationUpTick()
    else:
        SetTradeUpToZeero()


def TradePriceCalculationUpTick():
    ##Region "calculation for Price"
    global TradeOpenPriceUpTick
    global TradeClosePriceUpTick
    global TradeFrequencyUpTick
    global TradeMinPriceUpTick
    global TradeMaxPriceUpTick
    global TradeTickVolumeUpTick
    global TradeVolume
    global TradeCloseVolumeUpTick
    global TradeMinVolumeUpTick
    global TradeMaxVolumeUpTick
    global TradeTotalPriceUpTick
    global TradeTotalSizeUpTick
    global TradeTotalValueUpTick
    if TradeOpenPriceUpTick == 0:
        TradeOpenPriceUpTick = TradePrice
    TradeClosePriceUpTick = TradePrice
    TradeFrequencyUpTick = TradeFrequencyUpTick + 1
    TradeMinPriceUpTick = computeLow(TradePrice, TradeMinPriceUpTick)
    TradeMaxPriceUpTick = computeHigh(TradePrice, TradeMaxPriceUpTick)

    if TradeVolume > TradeCloseVolumeUpTick:
        TradeTickVolumeUpTick = TradeTickVolumeUpTick + 1
    elif TradeVolume < TradeCloseVolumeUpTick:
        TradeTickVolumeUpTick = TradeTickVolumeUpTick - 1
    TradeCloseVolumeUpTick = TradeVolume
    TradeMinVolumeUpTick = computeLow(TradeVolume, TradeMinVolumeUpTick)
    TradeMaxVolumeUpTick = computeHigh(TradeVolume, TradeMaxVolumeUpTick)
    TradeTotalPriceUpTick = TradeTotalPriceUpTick + TradePrice
    TradeTotalSizeUpTick = TradeTotalSizeUpTick + TradeVolume
    TradeTotalValueUpTick = TradeTotalValueUpTick + (TradeClosePriceUpTick * TradeVolume)


def TradePriceVolumeCalculationUpTick():
    global TradeTotalSizeUpTick
    global TradePriceVolumeWeightedUpTick
    global TradeTotalPriceUpTick
    global TradeVolumePriceWeightedUpTick
    global TradeFrequencyUpTick
    global ValuePerTradeUpTick

    if TradeTotalSizeUpTick > 0:
        TradePriceVolumeWeightedUpTick = TradeTotalValueUpTick / TradeTotalSizeUpTick
    if TradeTotalPriceUpTick > 0:
        TradeVolumePriceWeightedUpTick = TradeTotalValueUpTick / TradeTotalPriceUpTick
    if TradeFrequencyUpTick > 0:
        ValuePerTradeUpTick = TradeTotalValueUpTick / TradeFrequencyUpTick
    else:
        ValuePerTradeUpTick = 0


def SetTradeUpToZeero():
    global TradeTickVolumeUpTick
    global TradeCloseVolumeUpTick
    global TradeMinVolumeUpTick
    global TradeMaxVolumeUpTick
    global TradeTotalSizeUpTick
    global TradeTotalValueUpTick
    global TradeOpenPriceUpTick
    global TradeMinPriceUpTick
    global TradeMaxPriceUpTick

    TradeTickVolumeUpTick = 0
    TradeCloseVolumeUpTick = 0
    TradeMinVolumeUpTick = 0
    TradeMaxVolumeUpTick = 0
    TradeTotalSizeUpTick = 0
    TradeTotalValueUpTick = 0
    TradeOpenPriceUpTick = TradeClosePriceUpTick
    TradeMinPriceUpTick = TradeClosePriceUpTick
    TradeMaxPriceUpTick = TradeClosePriceUpTick


def TradeDownCalculation():
    global TradeOpenPriceDownTick
    global TradeClosePriceDownTick
    global TradeCloseVolumeDownTick
    global TradeDownStdDeviation
    global TradeDownProbability
    global TradeDownMedian
    if len(TradeDownVolume) > 0 and len(TradeDownPrice) > 0:
        # 'TradeDownVolume_str = TradeDownVolume.Clone
        TotalLength = len(TradeDownVolume)

        TradeDownValue = [0 for x in range(TotalLength)]

        for i in range(0, TotalLength - 1):
            TradeDownValue[i] = TradeDownPrice[i] * TradeDownVolume[i]
        Median = CalculationForMedian(TradeDownValue)
        # Median=np.median(TradeDownValue)
        Deviation = CalculateStandardDeviation(TradeDownValue)
        # Deviation=np.std(TradeDownValue)
        UpCut = int(((MedianUpCutOff * TotalLength) / 100))
        DownCut = int(((MedianDownCutOff * TotalLength) / 100))

        TradeDownValue.sort
        TradeOpenPriceDownTick = TradeDownPrice[0]
        for i in range(0, TotalLength - 1):
            TradePrice = TradeDownPrice[i]
            TradeVolume = TradeDownVolume[i]
            TradeValue = TradePrice * TradeVolume
            for j in range(DownCut, TotalLength - UpCut - 1):
                if TradeValue == TradeDownValue[j]:
                    TradePriceCalculationDownTick()
                    break
        TradeClosePriceDownTick = TradeDownPrice[TotalLength - 1]
        TradeCloseVolumeDownTick = TradeDownVolume[TotalLength - 1]
        TradeDownStdDeviation = Deviation
        TradeDownMedian = Median
        if TotalLength > 0:
            TradeDownProbability = TradeDownProbability / TotalLength
        TradePriceVolumeCalculationDownTick()
    else:
        SetTradeDownToZeero()


def TradePriceCalculationDownTick():
    ##Region "calculation for Price"
    global TradeOpenPriceDownTick
    global TradeClosePriceDownTick
    global TradeFrequencyDownTick
    global TradeMinPriceDownTick
    global TradeMaxPriceDownTick
    global TradeTickVolumeDownTick
    global TradeVolume
    global TradeCloseVolumeDownTick
    global TradeMinVolumeDownTick
    global TradeMaxVolumeDownTick
    global TradeTotalPriceDownTick
    global TradeTotalSizeDownTick
    global TradeTotalValueDownTick
    if TradeOpenPriceDownTick == 0:
        TradeOpenPriceDownTick = TradePrice
    TradeClosePriceDownTick = TradePrice
    TradeFrequencyDownTick = TradeFrequencyDownTick + 1
    TradeMinPriceDownTick = computeLow(TradePrice, TradeMinPriceDownTick)
    TradeMaxPriceDownTick = computeHigh(TradePrice, TradeMaxPriceDownTick)

    if TradeVolume > TradeCloseVolumeDownTick:
        TradeTickVolumeDownTick = TradeTickVolumeDownTick + 1
    elif TradeVolume < TradeCloseVolumeDownTick:
        TradeTickVolumeDownTick = TradeTickVolumeDownTick - 1
    TradeCloseVolumeDownTick = TradeVolume
    TradeMinVolumeDownTick = computeLow(TradeVolume, TradeMinVolumeDownTick)
    TradeMaxVolumeDownTick = computeHigh(TradeVolume, TradeMaxVolumeDownTick)
    TradeTotalPriceDownTick = TradeTotalPriceDownTick + TradePrice
    TradeTotalSizeDownTick = TradeTotalSizeDownTick + TradeVolume
    TradeTotalValueDownTick = TradeTotalValueDownTick + (TradeClosePriceDownTick * TradeVolume)


def TradePriceVolumeCalculationDownTick():
    global TradeTotalSizeDownTick
    global TradePriceVolumeWeightedDownTick
    global TradeTotalPriceDownTick
    global TradeVolumePriceWeightedDownTick
    global TradeFrequencyDownTick
    global ValuePerTradeDownTick

    if TradeTotalSizeDownTick > 0:
        TradePriceVolumeWeightedDownTick = TradeTotalValueDownTick / TradeTotalSizeDownTick
    if TradeTotalPriceDownTick > 0:
        TradeVolumePriceWeightedDownTick = TradeTotalValueDownTick / TradeTotalPriceDownTick
    if TradeFrequencyDownTick > 0:
        ValuePerTradeDownTick = TradeTotalValueDownTick / TradeFrequencyDownTick
    else:
        ValuePerTradeDownTick = 0


def SetTradeDownToZeero():
    global TradeTickVolumeDownTick
    global TradeCloseVolumeDownTick
    global TradeMinVolumeDownTick
    global TradeMaxVolumeDownTick
    global TradeTotalSizeDownTick
    global TradeTotalValueDownTick
    global TradeOpenPriceDownTick
    global TradeMinPriceDownTick
    global TradeMaxPriceDownTick

    TradeTickVolumeDownTick = 0
    TradeCloseVolumeDownTick = 0
    TradeMinVolumeDownTick = 0
    TradeMaxVolumeDownTick = 0
    TradeTotalSizeDownTick = 0
    TradeTotalValueDownTick = 0
    TradeOpenPriceDownTick = TradeClosePriceDownTick
    TradeMinPriceDownTick = TradeClosePriceDownTick
    TradeMaxPriceDownTick = TradeClosePriceDownTick


def BidUpCalculation():
    global BidOpenPriceUpTick
    global BidClosePriceUpTick
    global BidCloseVolumeUpTick
    global BidUpStdDeviation
    global BidUpProbability
    global BidUpMedian
    if len(BidUpVolume) > 0 and len(BidUpPrice) > 0:
        # 'BidUpVolume_str = BidUpVolume.Clone
        TotalLength = len(BidUpVolume)

        BidUpValue = [0 for x in range(TotalLength)]

        for i in range(0, TotalLength - 1):
            BidUpValue[i] = BidUpPrice[i] * BidUpVolume[i]
        Median = CalculationForMedian(BidUpValue)
        # Median=np.median(BidUpValue)
        Deviation = CalculateStandardDeviation(BidUpValue)
        # Deviation=np.std(BidUpValue)
        UpCut = int(((MedianUpCutOff * TotalLength) / 100))
        DownCut = int(((MedianDownCutOff * TotalLength) / 100))

        BidUpValue.sort
        BidOpenPriceUpTick = BidUpPrice[0]
        for i in range(0, TotalLength - 1):
            BidPrice = BidUpPrice[i]
            BidVolume = BidUpVolume[i]
            BidValue = BidPrice * BidVolume
            for j in range(DownCut, TotalLength - UpCut - 1):
                if BidValue == BidUpValue[j]:
                    BidPriceCalculationUpTick()
                    break
        BidClosePriceUpTick = BidUpPrice[TotalLength - 1]
        BidCloseVolumeUpTick = BidUpVolume[TotalLength - 1]
        BidUpStdDeviation = Deviation
        BidUpMedian = Median
        if TotalLength > 0:
            BidUpProbability = BidUpProbability / TotalLength
        BidPriceVolumeCalculationUpTick()
    else:
        SetBidUpToZeero()


def BidPriceCalculationUpTick():
    ##Region "calculation for Price"
    global BidOpenPriceUpTick
    global BidClosePriceUpTick
    global BidFrequencyUpTick
    global BidMinPriceUpTick
    global BidMaxPriceUpTick
    global BidTickVolumeUpTick
    global BidVolume
    global BidCloseVolumeUpTick
    global BidMinVolumeUpTick
    global BidMaxVolumeUpTick
    global BidTotalPriceUpTick
    global BidTotalSizeUpTick
    global BidTotalValueUpTick
    if BidOpenPriceUpTick == 0:
        BidOpenPriceUpTick = BidPrice
    BidClosePriceUpTick = BidPrice
    BidFrequencyUpTick = BidFrequencyUpTick + 1
    BidMinPriceUpTick = computeLow(BidPrice, BidMinPriceUpTick)
    BidMaxPriceUpTick = computeHigh(BidPrice, BidMaxPriceUpTick)

    if BidVolume > BidCloseVolumeUpTick:
        BidTickVolumeUpTick = BidTickVolumeUpTick + 1
    elif BidVolume < BidCloseVolumeUpTick:
        BidTickVolumeUpTick = BidTickVolumeUpTick - 1
    BidCloseVolumeUpTick = BidVolume
    BidMinVolumeUpTick = computeLow(BidVolume, BidMinVolumeUpTick)
    BidMaxVolumeUpTick = computeHigh(BidVolume, BidMaxVolumeUpTick)
    BidTotalPriceUpTick = BidTotalPriceUpTick + BidPrice
    BidTotalSizeUpTick = BidTotalSizeUpTick + BidVolume
    BidTotalValueUpTick = BidTotalValueUpTick + (BidClosePriceUpTick * BidVolume)


def BidPriceVolumeCalculationUpTick():
    global BidTotalSizeUpTick
    global BidPriceVolumeWeightedUpTick
    global BidTotalPriceUpTick
    global BidVolumePriceWeightedUpTick
    global BidFrequencyUpTick
    global ValuePerBidUpTick

    if BidTotalSizeUpTick > 0:
        BidPriceVolumeWeightedUpTick = BidTotalValueUpTick / BidTotalSizeUpTick
    if BidTotalPriceUpTick > 0:
        BidVolumePriceWeightedUpTick = BidTotalValueUpTick / BidTotalPriceUpTick
    if BidFrequencyUpTick > 0:
        ValuePerBidUpTick = BidTotalValueUpTick / BidFrequencyUpTick
    else:
        ValuePerBidUpTick = 0


def SetBidUpToZeero():
    global BidTickVolumeUpTick
    global BidCloseVolumeUpTick
    global BidMinVolumeUpTick
    global BidMaxVolumeUpTick
    global BidTotalSizeUpTick
    global BidTotalValueUpTick
    global BidOpenPriceUpTick
    global BidMinPriceUpTick
    global BidMaxPriceUpTick

    BidTickVolumeUpTick = 0
    BidCloseVolumeUpTick = 0
    BidMinVolumeUpTick = 0
    BidMaxVolumeUpTick = 0
    BidTotalSizeUpTick = 0
    BidTotalValueUpTick = 0
    BidOpenPriceUpTick = BidClosePriceUpTick
    BidMinPriceUpTick = BidClosePriceUpTick
    BidMaxPriceUpTick = BidClosePriceUpTick


def BidDownCalculation():
    global BidOpenPriceDownTick
    global BidClosePriceDownTick
    global BidCloseVolumeDownTick
    global BidDownStdDeviation
    global BidDownProbability
    global BidDownMedian
    if len(BidDownVolume) > 0 and len(BidDownPrice) > 0:
        # 'BidDownVolume_str = BidDownVolume.Clone
        TotalLength = len(BidDownVolume)

        BidDownValue = [0 for x in range(TotalLength)]

        for i in range(0, TotalLength - 1):
            BidDownValue[i] = BidDownPrice[i] * BidDownVolume[i]
        Median = CalculationForMedian(BidDownValue)
        # Median=np.median(BidDownValue)
        Deviation = CalculateStandardDeviation(BidDownValue)
        # Deviation=np.std(BidDownValue)
        UpCut = int(((MedianUpCutOff * TotalLength) / 100))
        DownCut = int(((MedianDownCutOff * TotalLength) / 100))

        BidDownValue.sort
        BidOpenPriceDownTick = BidDownPrice[0]
        for i in range(0, TotalLength - 1):
            BidPrice = BidDownPrice[i]
            BidVolume = BidDownVolume[i]
            BidValue = BidPrice * BidVolume
            for j in range(DownCut, TotalLength - UpCut - 1):
                if BidValue == BidDownValue[j]:
                    BidPriceCalculationDownTick()
                    break
        BidClosePriceDownTick = BidDownPrice[TotalLength - 1]
        BidCloseVolumeDownTick = BidDownVolume[TotalLength - 1]
        BidDownStdDeviation = Deviation
        BidDownMedian = Median
        if TotalLength > 0:
            BidDownProbability = BidDownProbability / TotalLength
        BidPriceVolumeCalculationDownTick()
    else:
        SetBidDownToZeero()


def BidPriceCalculationDownTick():
    ##Region "calculation for Price"
    global BidOpenPriceDownTick
    global BidClosePriceDownTick
    global BidFrequencyDownTick
    global BidMinPriceDownTick
    global BidMaxPriceDownTick
    global BidTickVolumeDownTick
    global BidVolume
    global BidCloseVolumeDownTick
    global BidMinVolumeDownTick
    global BidMaxVolumeDownTick
    global BidTotalPriceDownTick
    global BidTotalSizeDownTick
    global BidTotalValueDownTick
    if BidOpenPriceDownTick == 0:
        BidOpenPriceDownTick = BidPrice
    BidClosePriceDownTick = BidPrice
    BidFrequencyDownTick = BidFrequencyDownTick + 1
    BidMinPriceDownTick = computeLow(BidPrice, BidMinPriceDownTick)
    BidMaxPriceDownTick = computeHigh(BidPrice, BidMaxPriceDownTick)

    if BidVolume > BidCloseVolumeDownTick:
        BidTickVolumeDownTick = BidTickVolumeDownTick + 1
    elif BidVolume < BidCloseVolumeDownTick:
        BidTickVolumeDownTick = BidTickVolumeDownTick - 1
    BidCloseVolumeDownTick = BidVolume
    BidMinVolumeDownTick = computeLow(BidVolume, BidMinVolumeDownTick)
    BidMaxVolumeDownTick = computeHigh(BidVolume, BidMaxVolumeDownTick)
    BidTotalPriceDownTick = BidTotalPriceDownTick + BidPrice
    BidTotalSizeDownTick = BidTotalSizeDownTick + BidVolume
    BidTotalValueDownTick = BidTotalValueDownTick + (BidClosePriceDownTick * BidVolume)


def BidPriceVolumeCalculationDownTick():
    global BidTotalSizeDownTick
    global BidPriceVolumeWeightedDownTick
    global BidTotalPriceDownTick
    global BidVolumePriceWeightedDownTick
    global BidFrequencyDownTick
    global ValuePerBidDownTick

    if BidTotalSizeDownTick > 0:
        BidPriceVolumeWeightedDownTick = BidTotalValueDownTick / BidTotalSizeDownTick
    if BidTotalPriceDownTick > 0:
        BidVolumePriceWeightedDownTick = BidTotalValueDownTick / BidTotalPriceDownTick
    if BidFrequencyDownTick > 0:
        ValuePerBidDownTick = BidTotalValueDownTick / BidFrequencyDownTick
    else:
        ValuePerBidDownTick = 0


def SetBidDownToZeero():
    global BidTickVolumeDownTick
    global BidCloseVolumeDownTick
    global BidMinVolumeDownTick
    global BidMaxVolumeDownTick
    global BidTotalSizeDownTick
    global BidTotalValueDownTick
    global BidOpenPriceDownTick
    global BidMinPriceDownTick
    global BidMaxPriceDownTick

    BidTickVolumeDownTick = 0
    BidCloseVolumeDownTick = 0
    BidMinVolumeDownTick = 0
    BidMaxVolumeDownTick = 0
    BidTotalSizeDownTick = 0
    BidTotalValueDownTick = 0
    BidOpenPriceDownTick = BidClosePriceDownTick
    BidMinPriceDownTick = BidClosePriceDownTick
    BidMaxPriceDownTick = BidClosePriceDownTick


##Ask Up & Down Calculation Starts

def AskUpCalculation():
    global AskOpenPriceUpTick
    global AskClosePriceUpTick
    global AskCloseVolumeUpTick
    global AskUpStdDeviation
    global AskUpProbability
    global AskUpMedian
    if len(AskUpVolume) > 0 and len(AskUpPrice) > 0:
        # 'AskUpVolume_str = AskUpVolume.Clone
        TotalLength = len(AskUpVolume)

        AskUpValue = [0 for x in range(TotalLength)]

        for i in range(0, TotalLength - 1):
            AskUpValue[i] = AskUpPrice[i] * AskUpVolume[i]
        Median = CalculationForMedian(AskUpValue)
        # Median=np.median(AskUpValue)
        Deviation = CalculateStandardDeviation(AskUpValue)
        # Deviation=np.std(AskUpValue)
        UpCut = int(((MedianUpCutOff * TotalLength) / 100))
        DownCut = int(((MedianDownCutOff * TotalLength) / 100))

        AskUpValue.sort
        AskOpenPriceUpTick = AskUpPrice[0]
        for i in range(0, TotalLength - 1):
            AskPrice = AskUpPrice[i]
            AskVolume = AskUpVolume[i]
            AskValue = AskPrice * AskVolume
            for j in range(DownCut, TotalLength - UpCut - 1):
                if AskValue == AskUpValue[j]:
                    AskPriceCalculationUpTick()
                    break
        AskClosePriceUpTick = AskUpPrice[TotalLength - 1]
        AskCloseVolumeUpTick = AskUpVolume[TotalLength - 1]
        AskUpStdDeviation = Deviation
        AskUpMedian = Median
        if TotalLength > 0:
            AskUpProbability = AskUpProbability / TotalLength
        AskPriceVolumeCalculationUpTick()
    else:
        SetAskUpToZeero()


def AskPriceCalculationUpTick():
    ##Region "calculation for Price"
    global AskOpenPriceUpTick
    global AskClosePriceUpTick
    global AskFrequencyUpTick
    global AskMinPriceUpTick
    global AskMaxPriceUpTick
    global AskTickVolumeUpTick
    global AskVolume
    global AskCloseVolumeUpTick
    global AskMinVolumeUpTick
    global AskMaxVolumeUpTick
    global AskTotalPriceUpTick
    global AskTotalSizeUpTick
    global AskTotalValueUpTick
    if AskOpenPriceUpTick == 0:
        AskOpenPriceUpTick = AskPrice
    AskClosePriceUpTick = AskPrice
    AskFrequencyUpTick = AskFrequencyUpTick + 1
    AskMinPriceUpTick = computeLow(AskPrice, AskMinPriceUpTick)
    AskMaxPriceUpTick = computeHigh(AskPrice, AskMaxPriceUpTick)

    if AskVolume > AskCloseVolumeUpTick:
        AskTickVolumeUpTick = AskTickVolumeUpTick + 1
    elif AskVolume < AskCloseVolumeUpTick:
        AskTickVolumeUpTick = AskTickVolumeUpTick - 1
    AskCloseVolumeUpTick = AskVolume
    AskMinVolumeUpTick = computeLow(AskVolume, AskMinVolumeUpTick)
    AskMaxVolumeUpTick = computeHigh(AskVolume, AskMaxVolumeUpTick)
    AskTotalPriceUpTick = AskTotalPriceUpTick + AskPrice
    AskTotalSizeUpTick = AskTotalSizeUpTick + AskVolume
    AskTotalValueUpTick = AskTotalValueUpTick + (AskClosePriceUpTick * AskVolume)


def AskPriceVolumeCalculationUpTick():
    global AskTotalSizeUpTick
    global AskPriceVolumeWeightedUpTick
    global AskTotalPriceUpTick
    global AskVolumePriceWeightedUpTick
    global AskFrequencyUpTick
    global ValuePerAskUpTick

    if AskTotalSizeUpTick > 0:
        AskPriceVolumeWeightedUpTick = AskTotalValueUpTick / AskTotalSizeUpTick
    if AskTotalPriceUpTick > 0:
        AskVolumePriceWeightedUpTick = AskTotalValueUpTick / AskTotalPriceUpTick
    if AskFrequencyUpTick > 0:
        ValuePerAskUpTick = AskTotalValueUpTick / AskFrequencyUpTick
    else:
        ValuePerAskUpTick = 0


def SetAskUpToZeero():
    global AskTickVolumeUpTick
    global AskCloseVolumeUpTick
    global AskMinVolumeUpTick
    global AskMaxVolumeUpTick
    global AskTotalSizeUpTick
    global AskTotalValueUpTick
    global AskOpenPriceUpTick
    global AskMinPriceUpTick
    global AskMaxPriceUpTick

    AskTickVolumeUpTick = 0
    AskCloseVolumeUpTick = 0
    AskMinVolumeUpTick = 0
    AskMaxVolumeUpTick = 0
    AskTotalSizeUpTick = 0
    AskTotalValueUpTick = 0
    AskOpenPriceUpTick = AskClosePriceUpTick
    AskMinPriceUpTick = AskClosePriceUpTick
    AskMaxPriceUpTick = AskClosePriceUpTick


def AskDownCalculation():
    global AskOpenPriceDownTick
    global AskClosePriceDownTick
    global AskCloseVolumeDownTick
    global AskDownStdDeviation
    global AskDownProbability
    global AskDownMedian
    if len(AskDownVolume) > 0 and len(AskDownPrice) > 0:
        # 'AskDownVolume_str = AskDownVolume.Clone
        TotalLength = len(AskDownVolume)
        # ' AskDownVolume = AskDownVolume.Clone ''(TotalLength ) As Double '' = New Double(TotalLength ) {}
        # 'AskDownPrice_str = AskDownPrice.Clone ''.Split(",".ToCharArray())
        # Dim AskDownPrice(TotalLength - 1) As Double '' = New Double(TotalLength ) {}
        # 'TotalLength1 = TotalLength - 1
        # AskDownValue = vbObjectInitialize((TotalLength,), Variant)
        AskDownValue = [0 for x in range(TotalLength)]

        for i in range(0, TotalLength - 1):
            AskDownValue[i] = AskDownPrice[i] * AskDownVolume[i]
        Median = CalculationForMedian(AskDownValue)
        # Median=np.median(AskDownValue)
        Deviation = CalculateStandardDeviation(AskDownValue)
        # Deviation=np.std(AskDownValue)
        UpCut = int(((MedianUpCutOff * TotalLength) / 100))
        DownCut = int(((MedianDownCutOff * TotalLength) / 100))

        AskDownValue.sort
        AskOpenPriceDownTick = AskDownPrice[0]
        for i in range(0, TotalLength - 1):
            AskPrice = AskDownPrice[i]
            AskVolume = AskDownVolume[i]
            AskValue = AskPrice * AskVolume
            for j in range(DownCut, TotalLength - UpCut - 1):
                if AskValue == AskDownValue[j]:
                    AskPriceCalculationDownTick()
                    break
        AskClosePriceDownTick = AskDownPrice[TotalLength - 1]
        AskCloseVolumeDownTick = AskDownVolume[TotalLength - 1]
        AskDownStdDeviation = Deviation
        AskDownMedian = Median
        if TotalLength > 0:
            AskDownProbability = AskDownProbability / TotalLength
        AskPriceVolumeCalculationDownTick()
    else:
        SetAskDownToZeero()


def AskPriceCalculationDownTick():
    ##Region "calculation for Price"
    global AskOpenPriceDownTick
    global AskClosePriceDownTick
    global AskFrequencyDownTick
    global AskMinPriceDownTick
    global AskMaxPriceDownTick
    global AskTickVolumeDownTick
    global AskVolume
    global AskCloseVolumeDownTick
    global AskMinVolumeDownTick
    global AskMaxVolumeDownTick
    global AskTotalPriceDownTick
    global AskTotalSizeDownTick
    global AskTotalValueDownTick
    if AskOpenPriceDownTick == 0:
        AskOpenPriceDownTick = AskPrice
    AskClosePriceDownTick = AskPrice
    AskFrequencyDownTick = AskFrequencyDownTick + 1
    AskMinPriceDownTick = computeLow(AskPrice, AskMinPriceDownTick)
    AskMaxPriceDownTick = computeHigh(AskPrice, AskMaxPriceDownTick)

    if AskVolume > AskCloseVolumeDownTick:
        AskTickVolumeDownTick = AskTickVolumeDownTick + 1
    elif AskVolume < AskCloseVolumeDownTick:
        AskTickVolumeDownTick = AskTickVolumeDownTick - 1
    AskCloseVolumeDownTick = AskVolume
    AskMinVolumeDownTick = computeLow(AskVolume, AskMinVolumeDownTick)
    AskMaxVolumeDownTick = computeHigh(AskVolume, AskMaxVolumeDownTick)
    AskTotalPriceDownTick = AskTotalPriceDownTick + AskPrice
    AskTotalSizeDownTick = AskTotalSizeDownTick + AskVolume
    AskTotalValueDownTick = AskTotalValueDownTick + (AskClosePriceDownTick * AskVolume)


def AskPriceVolumeCalculationDownTick():
    global AskTotalSizeDownTick
    global AskPriceVolumeWeightedDownTick
    global AskTotalPriceDownTick
    global AskVolumePriceWeightedDownTick
    global AskFrequencyDownTick
    global ValuePerAskDownTick

    if AskTotalSizeDownTick > 0:
        AskPriceVolumeWeightedDownTick = AskTotalValueDownTick / AskTotalSizeDownTick
    if AskTotalPriceDownTick > 0:
        AskVolumePriceWeightedDownTick = AskTotalValueDownTick / AskTotalPriceDownTick
    if AskFrequencyDownTick > 0:
        ValuePerAskDownTick = AskTotalValueDownTick / AskFrequencyDownTick
    else:
        ValuePerAskDownTick = 0


def SetAskDownToZeero():
    global AskTickVolumeDownTick
    global AskCloseVolumeDownTick
    global AskMinVolumeDownTick
    global AskMaxVolumeDownTick
    global AskTotalSizeDownTick
    global AskTotalValueDownTick
    global AskOpenPriceDownTick
    global AskMinPriceDownTick
    global AskMaxPriceDownTick

    AskTickVolumeDownTick = 0
    AskCloseVolumeDownTick = 0
    AskMinVolumeDownTick = 0
    AskMaxVolumeDownTick = 0
    AskTotalSizeDownTick = 0
    AskTotalValueDownTick = 0
    AskOpenPriceDownTick = AskClosePriceDownTick
    AskMinPriceDownTick = AskClosePriceDownTick
    AskMaxPriceDownTick = AskClosePriceDownTick


def devidematrix(a, b):
    nRows = len(a)
    # nColumns=len(a[0])
    Matrix = [0 for y in range(nRows)]
    for i in range(0, nRows):
        Matrix[i] = a[i] / b[i]
    return Matrix


#### Main Code starts from Here


##Lambda Implementatio
def IvOiCSVtoArraylmbda(bucket, key, stp):
    keystp = 'Controllers/STPKeys.csv'
    MatrixSTP = CSVtoPRMTRlmbda(bucket, keystp)
    nRows = len(MatrixSTP)
    nColumns = len(MatrixSTP[0])
    for x in range(0, nRows):
        # print("print STP:", MatrixSTP[x][1])
        # print("print stp:", stp)
        for y in range(0, nColumns):
            if (stp == str(MatrixSTP[x][y])):
                rowindex = x - 1
                columnindex = y

    # print("Row, Column", rowindex, columnindex)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        contents = response['Body'].read()
        lines = contents.splitlines()
        nRows = len(lines)
        nColumns = 23
        # print("nRows: ", nRows)
        Matrix = [[0 for x in range(nColumns - 1)] for y in range(nRows - 1)]
        lstrt = 0
        for line in lines:
            lstrt = lstrt + 1
            parts = line.split(",")
            if (lstrt == rowindex + 2):
                # print("lstrt & rowindex", lstrt, rowindex)
                strt = 0
                for part in parts:
                    strt = strt + 1
                    if (strt >= 5):
                        try:
                            lpart = float(part)
                        except:
                            lpart = 0
                        # print('lstrt, strt, lpart', lstrt, strt, lpart)
                        Matrix[lstrt - 2][strt - 2] = lpart
        # print("Writing Matrix for lstrt and selectfrom",  Matrix)
        # rMatrix=[row[0:nColumns-1] for row in Matrix[rowindex:rowindex+1]]
        rMatrix = [row[4:nColumns] for row in Matrix[rowindex:rowindex + 1]]
        # print("Writing rMatrix ", rMatrix)
        return rMatrix[0]

    except Exception as e:
        print(e)


def TickCSVtoMatrixlmbda(bucket, key, LastDateTime):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        contents = response['Body'].read()
        lines = contents.splitlines()
        nRows = len(lines)
        nColumns = 8
        selectfrom = nRows
        # print("nRows: ", nRows)
        Matrix = [[0 for x in range(nColumns - 1)] for y in range(nRows - 1)]
        lstrt = 0
        datecontrol = 0
        for line in lines:
            lstrt = lstrt + 1
            parts = line.split(",")
            if (lstrt > 1):
                strt = 0
                for part in parts:
                    strt = strt + 1
                    if (strt <= 2):
                        if (strt == 1):
                            dte = part
                        if (strt == 2):
                            tme = part
                            dtetme = dte + " " + tme
                            dtetme = datetime.datetime.strptime(dtetme, '%m/%d/%Y %H:%M:%S')

                            if (datecontrol == 0 and dtetme > LastDateTime):
                                datecontrol = 1
                                selectfrom = lstrt

                                Matrix[lstrt - 2][strt - 2] = dtetme
                                # print("DateTime in Matrix",  Matrix[lstrt-2][strt-2])
                            elif (datecontrol == 1):

                                Matrix[lstrt - 2][strt - 2] = dtetme
                                # print("DateTime in Matrix",  Matrix[lstrt-2][strt-2])
                    else:
                        try:
                            lpart = float(part)
                        except:
                            lpart = part

                        Matrix[lstrt - 2][strt - 2] = lpart
        # print("Writing rMatrix for lstrt and selectfrom", lstrt, selectfrom)
        rMatrix = [row[1:7] for row in Matrix[selectfrom:lstrt]]
        return rMatrix

    except Exception as e:
        rMatrix = [[0 for x in range(7)] for y in range(0)]
        return rMatrix
        print(e)
        print(
            'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                keyin, bucketin))
        raise e


def CSVtoPRMTRlmbda(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        contents = response['Body'].read()
        lines = contents.splitlines()
        nRows = len(lines)
        nColumns = len(lines[0])
        # print("PRMTR nRows: ", nRows)
        # print("PRMTR nColumns: ", nColumns)
        Matrix = [[0 for x in range(nColumns)] for y in range(nRows)]
        # print('Matrix', Matrix)
        i = -1
        for line in lines:
            i = i + 1
            # print('i', i)
            parts = line.split(",")
            j = -1
            for part in parts:
                j = j + 1
                # print('j', j)
                try:
                    lpart = float(part)
                except:
                    lpart = part
                Matrix[i][j] = lpart
        return Matrix

    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                keyin, bucketin))
        raise e


def MatrixToCSVWritelmbdaOne(bucket, MDPCR, Header, key, LastDateTime, CurrentDateTime, nos):
    # print("LastDateTime :", LastDateTime)
    # print("CurrentDateTime :", CurrentDateTime)

    nColumnsData = len(MDPCR)  ## Tells how many files to write
    # nColumnsHeader=len(Header) ## Tells how many files to write
    # print('nColumnsData, nColumnsHeader', nColumnsData, nColumnsHeader)
    nRows = 0
    verystart=1
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        contents = response['Body'].read()
        newbody=contents
        lines = contents.splitlines()
        nRows = len(lines) - 1
        print('nRows & nos', nRows, nos)
        #print('I am in Try')
    except Exception as e:
        verystart=0
        #print('I am in except and nos is: ', nos)
        newbody = 'DateTime'
        newbody = newbody + ',' + str(Header)
        newbody = newbody + '\r\n'
        newbody = newbody + str(CurrentDateTime)
        for j in range(nColumnsData):
            try:
                val = float(MDPCR[j])
            except Exception as e:
                val = 0
            newbody = newbody + ',' + str(val)
        newbody = newbody + '\r\n'
        if (nRows < nos):
            for x in range(int(nos - nRows - 1)):
                newbody = newbody + str(LastDateTime)
                for j in range(nColumnsData):
                    try:
                        val = float(MDPCR[j])
                    except Exception as e:
                        val = 0
                    newbody = newbody + ',' + str(val)
                newbody = newbody + '\r\n'
        s3.put_object(Bucket=bucket, Key=key, Body=newbody)
    if (nRows > nos):
        lstrt = 0
        for line in lines:
            # print('line', line)
            lstrt = lstrt + 1
            parts = line.split(",")
            # print('parts', parts)
            if (lstrt == 1):
                newbody = 'DateTime'
                for j in range(nColumnsData):
                    newbody = newbody + ',' + str(Header[j])
                newbody = newbody + '\r\n'
            if (lstrt > 1 and lstrt < nos):
                strt = 0
                for part in parts:
                    strt = strt + 1
                    if (strt == 1): newbody = newbody + str(part)
                    if (strt > 1):
                        try:
                            part = float(part)
                        except Exception as e:
                            part = 0
                        newbody = newbody + ',' + str(part)
                newbody = newbody + '\r\n'
        #print('newbody', newbody)
    if (nRows < nos and verystart==1):
        newbody = contents
        for x in range(int(nos - nRows - 1)):
            newbody = newbody + str(LastDateTime)
            for j in range(nColumnsData):
                try:
                    val = float(MDPCR[j])
                except Exception as e:
                    val = 0
                newbody = newbody + ',' + str(val)
            newbody = newbody + '\r\n'
        nRows=len(newbody.splitlines())-1
    if (nRows==nos):
        print('nRows & nos', nRows, nos)
    else:
        # print('newbody', newbody)
        newbody = newbody + str(CurrentDateTime)
        for j in range(nColumnsData):
            try:
                val = float(MDPCR[j])
            except Exception as e:
                val = 0
            newbody = newbody + ',' + str(val)
        newbody = newbody + '\r\n'
        # print('newbody', newbody)
    s3.put_object(Bucket=bucket, Key=key, Body=newbody)
  
