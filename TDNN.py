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
    str=str[lencompany+17:length]
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
    keyout = 'NN/' + companyname + '/TDNN/' + stp    + '/' + I +'.csv'
    keydate='Controllers/DateTime.csv'
    #print("field : " , fi)
    
    #updatemonitortozero(tablename,stp,companyname,f)
  
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
    #print('PRMTR', PRMTR)
    #print(xvalidation, epochs, Refresh)
    MatrixField=TickCSVtoMatrixlmbda(bucketin, keyfield)
    lendata=len(MatrixField)
    #print('MatrixField', MatrixField)
    MatrixOS=TickCSVtoMatrixlmbda(bucketin, keyos)
    lenos=len(MatrixOS)
    MatrixOS=MatrixOS[1:lenos]
    lenos=len(MatrixOS)
    diffos=lendata-lenos
    tdnndepth=12
    #print('lendata, lenos', lendata, lenos)
    data=prepdata(MatrixField,MatrixOS,tdnndepth)
    
    lendata=len(data)
    patt=data[1:lenos-1]
    random.shuffle(patt)
    inlayer=tdnndepth  #+1
    hidn=int(math.fabs(inlayer/6))
    if(hidn<1):hidn=1
    #print('hidn', hidn)
    myNN = NN ( inlayer, hidn, 1)
    myNN.train(patt)   
    
    pi=myNN.test(data)
    #print('PI', pi)
    #print('lendata and lenPI', lendata, len(pi))
    
    MatrixToCSVWritelmbda(bucketout, pi,keyout,tdnndepth)
    #tablename=companyname
    #outputpi(tablename,stp,pi,f)
    tablename='Monitor'
    #updatemonitortoone(tablename,stp,companyname,f)
    



    
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



    
def prepdata(TrainingData,TrainingosData,tdnndepth):
    lenos=len(TrainingosData)
    lenfield=len(TrainingData)
    #print('lenfield & lenos', lenfield, lenos)
    pat=[]
    i=-1
    
    for k in range(len(TrainingData)):
        #print('k:', k)
        if(k>=tdnndepth):
            i=i+1
            inp=[]
            for j in range(tdnndepth):
                #print('j:', j)
                #print('TrainingData[i+j]', TrainingData[i+j])
                a=str.split(TrainingData[i+j],',')
                
                
                #print('a2', a[1])
                inp.append(float(a[0]))
            
            #print('inp', inp)
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
    return pat

def MatrixToCSVWritelmbda(bucket, PI,key,tdnndepth):
    nRows=len(PI) ## Tells how many files to write
    #nColumnsHeader=len(Header) ## Tells how many files to write
    #print('nColumnsData, nColumnsHeader', nColumnsData, nColumnsHeader)
    nos=nRows+tdnndepth
    newbody=''
    for i in range(nos):
        if(i<tdnndepth):
            newbody=newbody + '0'
            newbody=newbody + '\r\n'
        else:
            newbody=newbody + str(PI[i-tdnndepth][0])
            newbody=newbody + '\r\n'
    s3.put_object(Bucket=bucket, Key=key, Body=newbody)
         
    
  
def outputpi(tablename,stp,pi,f):
   

    try:
        table = dynamodb.create_table(
            TableName=tablename,
            KeySchema=[
                {
                    'AttributeName': 'NN',
                    'KeyType': 'HASH'  #Partition key
                },
                {
                    'AttributeName': 'STP',
                    'KeyType': 'RANGE'  #Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'NN',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'STP',
                    'AttributeType': 'S'
                },
        
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        
        print("Table status:", table.table_status) 
        
    except Exception as e:
        table = dynamodb.Table(tablename)
        strpi=str(pi)
        #table.meta.client.get_waiter('table_exists').wait(TableName='table')
        with table.batch_writer() as batch:
            batch.put_item(
                Item={
                    'NN': 'TDNN',
                    'STP': stp,
                    f: strpi,
                    #f: decimal.Decimal(0.5),
                    }
                )
            
            

    
def updatemonitortozero(tablename,stp,companyname,f):
    table = dynamodb.Table(tablename)
    #table.meta.client.get_waiter('table_exists').wait(TableName='table')
    table.put_item(
        Item={
            'Company': companyname,
            'STP': stp,
            'Data': 1,
            'TDNN': 0,
            'Field':0
            }
        )
        
        

    
def updatemonitortoone(tablename,stp,companyname,f):
    table = dynamodb.Table(tablename)
    #table.meta.client.get_waiter('table_exists').wait(TableName='table')
    table.put_item(
        Item={
            'Company': companyname,
            'STP': stp,
            'Data': 1,
            'TDNN': 1,
            'Field':f
            }
        )