from __future__ import print_function
import json
import urllib
import re
import os,sys
import datetime
import math
from math import *
import csv
import time
import boto3
import random
import string
import decimal
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')  

def lambda_handler(event, context):
    bucketin = event['Records'][0]['s3']['bucket']['name']
    keyin = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')
    #print('bucketin, keyin', bucketin, keyin)
    keyfield=keyin
    bucketout = bucketin
    length=len(keyin)
    str=keyin[3:length]
    strlen=len(str)
    
    companyname=''
    slash='/'
    i=0
    while str[i] != slash:
        companyname=companyname + str[i]
        i=i+1
        
    #print("companyname : " , companyname) 
    lencompany=len(companyname)
    str=str[lencompany+1:length]
    #print('str', str)
    stp=''
    i=0
    while str[i] != slash:
        stp=stp+str[i]
        i=i+1
    #print("STP : ",  stp)  
    lenstp=len(stp)
    str=str[lenstp+1:length]
    f=''
    I=''
    slash='.'
    i=0
    while str[i] != slash:
        f=f+str[i]
        I=I+str[i]
        i=i+1
    #print("field : " , I)
    
    keyos= 'OptimalSignal/' + companyname + '.csv'
    keyout = 'NN/' + companyname + '/JordonSTPCentroids/' + I +'.csv'
    
    #print("field : " , fi)
    
   
  
    #print('keyos', keyos)
    #print('keyout', keyout)
   
    #keyprmtr = 'Controllers/PRMTR.csv' # + keyin[26:length]   
    #PRMTR=TickCSVtoMatrixlmbda(bucketin, keyprmtr)
    #print('PRMTR', PRMTR)
    #PRMTR=PRMTR[1]
    #print('PRMTR', PRMTR)
    #PRMTR=PRMTR.split(",")
    
    crossvalidation=40
    TDNNEpochs=25
    TDNNReset=1
    tdnndepth=12
    #print('PRMTR', PRMTR)
    #print(xvalidation, epochs, Refresh)
    MatrixField=TickCSVtoMatrixlmbda(bucketin, keyfield)
    lendata=len(MatrixField)
    MatrixField=MatrixField[tdnndepth:lendata]
    line=MatrixField[0]
    a=line.split(',')
    numfld=len(a)
    #print('MatrixField', MatrixField)
    MatrixOS=TickCSVtoMatrixlmbda(bucketin, keyos)
    
    lenos=len(MatrixOS)
    MatrixOS=MatrixOS[1:lenos]
    MatrixOS=MatrixOS[tdnndepth:lenos]
    lenos=len(MatrixOS)
    #print('lendata, lenos', lendata, lenos)
    diffos=lendata-lenos
    
    
    data=prepdata(MatrixField,MatrixOS)
    
    lendata=len(data)
    #print('lendata',lendata)
    patt=data[1:lenos-1]
    #patt=data[1:8]
    #print('original patt', patt)
    random.shuffle(patt)
    #print('reshuffle patt',patt)
    inlayer=numfld
    hidn=int(math.fabs(inlayer/4))
    if(hidn<1):hidn=1
    #print('hidn', hidn)
    myNN = NN ( inlayer, hidn, 1)
    myNN.train(patt)   
    
    pi=myNN.test(data)
    #print('PI', pi)
    #print('lendata and lenPI', lendata, len(pi))
    
    MatrixToCSVWritelmbda(bucketout, pi,keyout, tdnndepth)
    
    



    
#functions start from here    
    
def TickCSVtoMatrixlmbda(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)

        contents = response['Body'].read()
        lines = contents.splitlines()
        return lines
    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                keyin, bucketin))
        raise e
        


random.seed(0)

# calculate a random number where:  a <= rand < b
def rand(a, b):
    return (b-a)*random.random() + a

# Make a matrix (we could use NumPy to speed this up)
def makeMatrix(I, J, fill=0.0):
    m = []
    for i in range(I):
        m.append([fill]*J)
    return m

# our sigmoid function, tanh is a little nicer than the standard 1/(1+e^-x)
def sigmoid(x):
    #print('x, sigmoid(x)', x, math.tanh(x))
    return math.tanh(x)

# derivative of our sigmoid function, in terms of the output (i.e. y)
def dsigmoid(y):
    return 1.0 - y**2

class NN:
    def __init__(self, ni, nh, no):
        # number of input, hidden, and output nodes
        self.ni = ni + 1 # +1 for bias node
        self.nh = nh
        self.no = no

        # activations for nodes
        self.ai = [1.0]*self.ni
        self.ah = [1.0]*self.nh
        self.ao = [1.0]*self.no
        
        # create weights
        self.wi = makeMatrix(self.ni, self.nh)
        self.wo = makeMatrix(self.nh, self.no)
        
        # set them to random vaules
        for i in range(self.ni):
            for j in range(self.nh):
                self.wi[i][j] = rand(-0.2, 0.2)
        for j in range(self.nh):
            for k in range(self.no):
                self.wo[j][k] = rand(-0.2, 0.2)
                
        self.wis=self.wi
        self.wos=self.wo

        # last change in weights for momentum   
        self.ci = makeMatrix(self.ni, self.nh)
        self.co = makeMatrix(self.nh, self.no)

    def update(self, inputs):
        if len(inputs) != self.ni-1:
            raise ValueError('wrong number of inputs')

        # input activations
        for i in range(self.ni-1):
            #self.ai[i] = sigmoid(inputs[i])
            self.ai[i] = inputs[i]
            #print('input', inputs[i])

        # hidden activations
        for j in range(self.nh):
            sum = 0.0
            for i in range(self.ni):
                sum = sum + self.ai[i] * self.wi[i][j]
            self.ah[j] = sigmoid(sum)
            #print('sum', sum)

        # output activations
        for k in range(self.no):
            sum = 0.0
            for j in range(self.nh):
                sum = sum + self.ah[j] * self.wo[j][k]
            self.ao[k] = sigmoid(sum)
            #print('sum, sigmoidoutput', sum,self.ao[k])

        return self.ao[:]
        
    def predict(self, inputs):
        if len(inputs) != self.ni-1:
            raise ValueError('wrong number of inputs')

        # input activations
        for i in range(self.ni-1):
            #self.ai[i] = sigmoid(inputs[i])
            self.ai[i] = inputs[i]
            #print('input', inputs[i])

        # hidden activations
        for j in range(self.nh):
            sum = 0.0
            for i in range(self.ni):
                sum = sum + self.ai[i] * self.wis[i][j]
            self.ah[j] = sigmoid(sum)
            #print('sum', sum)

        # output activations
        for k in range(self.no):
            sum = 0.0
            for j in range(self.nh):
                sum = sum + self.ah[j] * self.wos[j][k]
            self.ao[k] = sigmoid(sum)
            #print('sum, sigmoidoutput', sum,self.ao[k])

        return self.ao[:]
        
     


    def backPropagate(self, targets, N, M,catchwi,i):
        if len(targets) != self.no:
            raise ValueError('wrong number of target values')
        if(catchwi==1 or i==0):
            self.wis=self.wi
            self.wos=self.wo
            #print('catchwi, i', catchwi, i)
            
        # calculate error terms for output
        output_deltas = [0.0] * self.no
        for k in range(self.no):
            #print('iteration, targets, predicted', i, targets[k], self.ao[k])
            error = targets[k]-self.ao[k]
            output_deltas[k] = dsigmoid(self.ao[k]) * error

        # calculate error terms for hidden
        hidden_deltas = [0.0] * self.nh
        for j in range(self.nh):
            error = 0.0
            for k in range(self.no):
                error = error + output_deltas[k]*self.wo[j][k]
            hidden_deltas[j] = dsigmoid(self.ah[j]) * error

        # update output weights
        for j in range(self.nh):
            for k in range(self.no):
                change = output_deltas[k]*self.ah[j]
                self.wo[j][k] = self.wo[j][k] + N*change + M*self.co[j][k]
                self.co[j][k] = change
                #print N*change, M*self.co[j][k]

        # update input weights
        for i in range(self.ni):
            for j in range(self.nh):
                change = hidden_deltas[j]*self.ai[i]
                self.wi[i][j] = self.wi[i][j] + N*change + M*self.ci[i][j]
                self.ci[i][j] = change

        # calculate error
        error = 0.0
        for k in range(len(targets)):
            error = error + 0.5*(targets[k]-self.ao[k])**2
        return error


    def test(self, patterns):
        i=0
        predictedIndicator = [0 for y in range(len(patterns))] 
        for p in patterns:
            
            #xx
            predictedIndicator[i]=self.predict(p[0])
            i=i+1
        return predictedIndicator

    def weights(self):
        print('Input weights:')
        for i in range(self.ni):
            print(self.wi[i])
        print()
        print('Output weights:')
        for j in range(self.nh):
            print(self.wo[j])

    def train(self, patterns, iterations=20, N=0.5, M=0.2):
        # N: learning rate
        # M: momentum factor
        bestepoch=0
        lenp=len(patterns)
        crossvalidation=40
        ValidationRow =long( math.fabs(lenp * float(crossvalidation) / 100))
        #print('lenp, ValidationRow', lenp, ValidationRow)
        tpatterns=patterns[ValidationRow:lenp]
        #print('tpatterns', tpatterns)
        vpatterns=patterns[0:ValidationRow]
        #print('vpatterns', vpatterns)
        tverror=[0 for   y in range(iterations)] 
        catchwi=0
        for i in range(iterations):
            
            errort = 0.0
            for p in tpatterns:
                inputs = p[0]
                targets = p[1]
                self.update(inputs)
                errort = errort + self.backPropagate(targets, N, M,catchwi,i)
            catchwi=0
            
            errorv=0.0
            j=0
            for p in vpatterns:
                
                pi=self.update(p[0])
                errorv = errorv + 0.5*(vpatterns[j][1][0]-pi[0])**2
                j=j+1
            tverror[i]=errorv + errort
            if(i==0):lastminerror=errorv + errort
            if(tverror[i] <=lastminerror):
                lastminerror=tverror[i] 
                catchwi=1
                bestepoch=i
                
                
            if i % 1 == 0:
                i=i
                #print('errort, errorv, tverror' , errort, errorv,tverror[i] )
        #print('bestepoch', bestepoch)



    
def prepdata(TrainingData,TrainingosData):
    lenos=len(TrainingosData)
    lenfield=len(TrainingData)
    #numfld=len(TrainingData[0])
    #print('lenfield & lenos & numfield', lenfield, lenos, numfld)
    
    
    
    pat=[]
    for k in range(len(TrainingData)):
        #print('k:', k)
        inp=[]
        a=str.split(TrainingData[k],',')
        for p in a:
            #print('a2', a[1])
            inp.append(float(p))
        
        if (k<lenos): 
            #print('k: and TrainingosData[k]', k, TrainingosData[k])
            b=str.split(TrainingosData[k],',')
            #print('b3', b[2])
            try:
                out=[float(b[2])]
            except Exception as e:
                out=[0.0]
        else:
            out=[0.0]
        inpout=[inp,out]
        pat.extend([inpout])
        #pat[i][0]=inp
    #print('len pat', len(pat))  
    #print('Pat', pat)
    return pat

def MatrixToCSVWritelmbda(bucket, PI,key, tdnndepth):
    nRows=len(PI) ## Tells how many files to write
    #nColumnsHeader=len(Header) ## Tells how many files to write
    #print('nColumnsData, nColumnsHeader', nColumnsData, nColumnsHeader)
    nos=nRows+tdnndepth
    newbody=''
    for i in range(nos):
        if(i<tdnndepth):newbody=newbody +str(0)
        else:newbody=newbody + str(PI[i-tdnndepth][0])
        newbody=newbody + '\r\n'
    s3.put_object(Bucket=bucket, Key=key, Body=newbody)
    
    
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
        if not (not ( NoExchanges )):
            break 
    return TempArray


def CalculationForMedian(Data_Series):
    Length = len(Data_Series) 
    #print(Length)
    #global Median    
    Series =  [0  for y in range(Length)]
    #'ReDim Series(10000)
    for i in range(0, Length - 1):
        Series[i] = Data_Series[i]   
    #Series = BubbleSort(Series)
    Series.sort
    #print('series', Series)
    mod2=math.fmod(Length,2)
    if Length == 0: Med = 0
    if Length == 1: Med = Series[0]   
    len2=int(Length/2)
    len2m1=int(len2-1)   
    if mod2 == 0: 
        Med = Series[len2] + Series[len2m1]
        Med=Med / 2    
    if mod2 == 1: Med = Series[len2]
    return Med
    
def CalculateStandardDeviation(Data_Series,Med):
    Total_numbers = len(Data_Series)
    Total_Variance=0   
    if Total_numbers == 0:
        Standard_Deviation = 0
    else:
        # Array.Sort(Data_Series);
        #Med = CalculationForMedian(Data_Series)
        for i in range(Total_numbers):
            Total_Variance = Total_Variance +  ( ( Data_Series[i] - Med )  *  ( Data_Series[i] - Med ) )
        Standard_Deviation = ( math.sqrt(( Total_Variance / Total_numbers )) )      
    return Standard_Deviation