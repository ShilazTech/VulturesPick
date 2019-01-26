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
import subprocess 

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
    #print("field : " , f)
    #print(ft)
    
    keyos= 'OptimalSignal/' + companyname + '.csv'
    keyout = 'NN/' + companyname + '/JordonCompany/' +companyname +'.csv'
    tablename='Monitor'
    keydate='Controllers/DateTime.csv'
    
    keyprmtr = 'Controllers/PRMTR.csv' # + keyin[26:length]
    PRMTR=CSVtoPRMTRlmbda(bucketin, keyprmtr)
    nos=PRMTR[1][1]
    
    crossvalidation=40
    TDNNEpochs=25
    TDNNReset=1
    #print('PRMTR', PRMTR)
    #print(xvalidation, epochs, Refresh)
    MatrixField=TickCSVtoMatrixlmbda(bucketin, keyfield,nos)
    MatrixDate=TickCSVtoMatrixlmbda(bucketin, keydate,0)
    lendata=len(MatrixField)
    #print('MatrixField', MatrixField)
    MatrixOS=TickCSVtoMatrixlmbda(bucketin, keyos,0)
    #print('MatrixOS', MatrixOS)
    lenos=len(MatrixOS)
    MatrixOS=MatrixOS[1:lenos]
    lenos=len(MatrixOS)
    #print('lendata, lenos', lendata, lenos)
    diffos=lendata-lenos
    tdnndepth=12
    
    
#K-Means starts here
    ifield=10
    data=prepkmeansdata(bucketin,keyfield,ifield,tdnndepth,nos)
    #print ('lendata', len(data))
    
    #print('hidn', hidn)
  
    # Training Samples
    num_points = len(data)
    # For each of those points how many dimensions do they have? 
    dimensions = ifield 
    # Bounds for the values of those points in each dimension 
    lower = 0 
    upper = 200 
    # The K in k-means. How many clusters do we assume exist? 
    num_clusters = 10
    # When do we say the optimization has 'converged' and stop updating clusters 
    opt_cutoff = 0.25 
    # Generate some points 
    #points = [makeRandomPoint(dimensions, lower, upper) for i in xrange(num_points)] 
    #print('lenpoints', len(points))
    # Cluster those data! 
    clusters = kmeans(data, num_clusters, opt_cutoff) 
    
    # Print our centroids
    #for i,c in enumerate(clusters):
    #    cp =c.centroid
    #    print(' Cluster: ', i, '\t centroid :', cp)
    #    print('lencp', cp.n)
    #    print('cp1 and cp32', cp.coords[0], cp.coords[131] )
        
    
    # Print our clusters 
    #for i,c in enumerate(clusters):
    #    for p in c.points: 
    #        print(' Cluster: ', i, '\t Point :', p)

# K-Means ends here    

    data=prepdata(clusters,num_clusters,MatrixOS, diffos,tdnndepth,MatrixDate)
    lendata=len(data)
    patt=data[1:lenos-1]
    random.shuffle(patt)
    inplayer=num_clusters
    hidn=int(math.fabs(inplayer/2))
    
    myNN = NN ( inplayer, hidn, 1)
    myNN.train(patt)   
    
    pi=myNN.test(data)
    #print('PI', pi)
    #print('lendata and lenPI', lendata, len(pi))
    
    MatrixToCSVWritelmbda(bucketout, pi,keyout,tdnndepth)
    #tablename=companyname
    #outputpi(tablename,stp,pi,f)
    tablename='Monitor'
    #updatemonitortoone(tablename,stp,companyname,f)
    #print("field : " , fi)



    
#functions start from here    
    
def TickCSVtoMatrixlmbda(bucket, key, nos):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        contents = response['Body'].read()
        lines = contents.splitlines()
        lendata=len(lines)
        #print
        while lendata<nos:
            time.sleep(2)
            response = s3.get_object(Bucket=bucket, Key=key)
            contents = response['Body'].read()
            lines = contents.splitlines()
            lendata=len(lines)
            #print('key, lendata, nos', key, lendata, nos)
            
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

def prepkmeansdata(bucketin,key, ifield,tdnndepth, nos):
    keylen=len(key)
    #print('key', key)
    keystart=key[0:keylen-6]
    #print('keystart', keystart)
    pat=[]
    for fi in range(ifield):
        keyfield=keystart + 'F' + str(fi) + '.csv'
        #print('keyfield', keyfield)
        TrainingData=(TickCSVtoMatrixlmbda(bucketin, keyfield, nos))
        lendata=len(TrainingData)
        TrainingData=TrainingData[tdnndepth:lendata]
        TrainingData = [float(x) for x in TrainingData]
        #map(float, TrainingData)
        #normalize=1 or not=0
        nrmlz=0
        if(nrmlz==1):
            fldsort=BubbleSort(TrainingData)
            med=CalculationForMedian(fldsort)
            vrn=CalculateStandardDeviation(fldsort,med) +4
            vrn=math.sqrt(vrn)
            #if (med==0): med=1
            nrm = [(float(i)-med)/vrn for i in TrainingData]
            p=Point(nrm)
        else:
            p=Point(TrainingData)
        pat.append(p)
    #keydate='Controllers/DateTime.csv'
    #response = s3.get_object(Bucket=bucketin, Key=keydate)
    #contents = response['Body'].read()
    #lines = contents.splitlines()
    #lendata=len(lines)
    #lines=lines[tdnndepth+1:lendata]
    #i=0
    #dy=[]
    #wkday=[]
    #hr=[]
    #mnt=[]
    #for line in lines:
    #    i=i+1
    #    if( i>0):
            #print('line', line)
    #        parts=line.split(",")
            #print('parts', parts)
            #print('parts[1]', parts[1])
    #        dy.append(float(parts[0]))
    #        wkday.append(float(parts[1]))
    #        hr.append(float(parts[2]))
    #        mnt.append(float(parts[3]))
    #pat.append(Point(dy)) 
    #pat.append(Point(wkday)) 
    #pat.append(Point(hr)) 
    #pat.append(Point(mnt))
    return pat
    
def prepdata(clusters,num_clusters,TrainingosData, diffos,tdnndepth,DateData):
    lenos=len(TrainingosData) -tdnndepth
    lendata=lenos + diffos
    #print('lenos', lenos)
    #print('lendata', lendata)
    
    pat=[]
    for k in range(lendata):
        #print('k:', k)
        inp=[]
        for i,c in enumerate(clusters):
            cp =c.centroid
            a=cp.coords[k]
            inp.append(float(a))
        #d=str.split(DateData[k+tdnndepth],',')
        #inp.append(float(d[0]))
        #inp.append(float(d[1]))
        #inp.append(float(d[2]))
        #inp.append(float(d[3]))
            #print('inp', inp)
        if (k<lenos): 
            #print('k: and TrainingosData[k]', k, TrainingosData[k])
            b=str.split(TrainingosData[k+tdnndepth],',')
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
    #print('pat', pat)          
    return pat


def prepkmeansdataold(bucketin,key, ifield,tdnndepth):
    keylen=len(key)
    keystart=key[0:keylen-8]
    #print('keystart', keystart)
    TrainingData=[]
    for fi in range(ifield):
        keyfield=keystart + 'F' + str(fi) + '.csv'
        #print('keyfield', keyfield)
        TrainingData.append(TickCSVtoMatrixlmbda(bucketin, keyfield))
        
    #print('TrainingData',  TrainingData)  
    lenfield=len(TrainingData[0])
    #print('lenfield',lenfield)
    #p=Point([])
    pat=[]
    for k in range(len(TrainingData[0])):
        #print('k:', k)
        if(k>=tdnndepth):
            #ip=Point([])
            inp=[]
            for j in range(ifield):
                a=TrainingData[j]
                #print('a2', a[1])
                inp.append(float(a[k]))
                #print('inp', inp)
               
            p=Point(inp)
            pat.append(p)
            #pat[i][0]=inp
    #print('pat', pat)          
    return pat
    
def prepdataold(bucketin,key,TrainingosData,tdnndepth, ifield):
    lenos=len(TrainingosData)
    
    #print('lenfield & lenos', lenfield, lenos)
    keylen=len(key)
    keystart=key[0:keylen-8]
    #print('keystart', keystart)
    TrainingData=[]
    for fi in range(ifield):
        keyfield=keystart + 'F' + str(fi) + '.csv'
        #print('keyfield', keyfield)
        TrainingData.append(TickCSVtoMatrixlmbda(bucketin, keyfield))
        
    #print('TrainingData',  TrainingData)  
    lenfield=len(TrainingData[0])
    #print('lenfield',lenfield)
    pat=[]
    for k in range(len(TrainingData[0])):
        #print('k:', k)
        if(k>=tdnndepth):
            inp=[]
            for j in range(ifield):
                a=TrainingData[j]
                #print('a2', a[1])
                inp.append(float(a[k]))
                #print('inp', inp)
            if (k<lenos): 
                #print('k: and TrainingosData[k]', k, TrainingosData[k])
                b=str.split(TrainingosData[k],',')
                #print('b3', b[2])
                out=[float(b[2])]
            else:
                out=[0.0]
            inpout=[inp,out]
            pat.extend([inpout])
            #pat[i][0]=inp
    #print('pat', pat)          
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
        
        
class Point:
    def __init__(self, coords):      
        self.coords = coords 
        self.n = len(coords) 
      
    def __repr__(self): 
        return str(self.coords) 
     
     
class Cluster:
    def __init__(self, points): 
     
        if len(points) == 0: raise Exception("ILLEGAL: empty cluster") 
        # The points that belong to this cluster 
        self.points = points
        

         
        # The dimensionality of the points in this cluster 
        self.n = points[0].n
        #self.n = len(points)
          
        # Assert that all points are of the same dimensionality 
        for p in points: 
            if p.n != self.n: raise Exception("ILLEGAL: wrong dimensions") 
              
        # Set up the initial centroid (this is usually based off one point) 
        self.centroid = self.calculateCentroid() 
        
    #def switch(self):
    #    self.points[0], self.points[1] = self.points[1], self.points[0]
  
    def __repr__(self): return str(self.points) 
      
    def update(self, points):  
        old_centroid = self.centroid 
        self.points = points 
        self.centroid = self.calculateCentroid() 
        shift = getDistance(old_centroid, self.centroid)  
        return shift 
      
    def calculateCentroid(self):
        numPoints = len(self.points) 
        # Get a list of all coordinates in this cluster 
        coords = [p.coords for p in self.points] 
        # Reformat that so all x's are together, all y'z etc. 
        unzipped = zip(*coords) 
        # Calculate the mean for each dimension 
        centroid_coords = [math.fsum(dList)/numPoints for dList in unzipped] 
          
        return Point(centroid_coords) 
     
     
def kmeans(points, k, cutoff): 
  
    # Pick out k random points to use as our initial centroids 
    initial = random.sample(points, k)
    
    #for ik in range(k):
    #    print('initial', initial[ik]) 
    #    #print('lencentroid', len(initial[ik]))
    # Create k clusters using those centroids 
    clusters = [Cluster([p]) for p in initial] 
    #ilists=clusters = [Cluster([p]) for p in initial] 
    #print('clusters', clusters) 
    lenC=len(clusters) 
    #print('lenC', lenC) 
    # Loop through the dataset until the clusters stabilize 
    loopCounter = 0 
    while True: 
        # Create a list of lists to hold the points in each cluster 
        lists = [ [] for c in clusters] 
        #lists=ilists
        #print('lists', lists)
        #lists=[]
        clusterCount = len(clusters) 
        #print('clustercount', clusterCount)
        # Start counting loops 
        loopCounter += 1 
        # For every point in the dataset ... 
        k=0
        for p in points: 
            k=k+1
            # Get the distance between that point and the centroid of the first 
            # cluster. 
            smallest_distance = getDistance(p, clusters[0].centroid)
            #print('smallest_distance :', smallest_distance)
          
            # Set the cluster this point belongs to 
            clusterIndex = 0 
            
            #lists[clusterIndex].append(p) 
          
            # For the remainder of the clusters ... 
            for i in range(clusterCount-1): 
                # calculate the distance of that point to each other cluster's 
                # centroid. 
                distance = getDistance(p, clusters[i+1].centroid) 
                #print('distance :', distance, 'i', i, 'k: ', k)
                # If it's closer to that cluster's centroid update what we 
                # think the smallest distance is, and set the point to belong 
                # to that cluster 
                #print('smallest_distance and distance', smallest_distance, distance)
                if distance < smallest_distance: 
                    smallest_distance = distance 
                    clusterIndex = i+1 
            #print('clusterIndex', clusterIndex)
            lists[clusterIndex].append(p) 
            #lists.append(p) 
        #lists=[lists]
        #print('lists', lists)
        #print('lenlists', len(lists[0]))
          
        # Set our biggest_shift to zero for this iteration 
        biggest_shift = 0.0 
          
        # As many times as there are clusters ... 
        for i in range(clusterCount): 
            # Calculate how far the centroid moved in this iteration 
            #print('p :', p, 'i :' , i, 'loopCounter :', loopCounter, 'len Points:', lenp)
            #shift = clusters[i].update(lists[i][0]) 
            #print('lists[i]', lists[i])
            lenl=len(lists[i])
            
            if (lenl==0):
                #print('lenl :', lenl)
                lists[i].append(initial[i])
            shift = clusters[i].update(lists[i]) 
            # Keep track of the largest move from all cluster centroid updates 
            biggest_shift = max(biggest_shift, shift) 
          
        # If the centroids have stopped moving much, say we're done! 
        if biggest_shift < cutoff: 
            #print ("Converged after %s iterations" % loopCounter)
            break 
    #for i in range(clusterCount): 
    #    initial[i]=self.calculateCentroid() 
        
    return clusters 
    #return initial 
 
 
def getDistance(a, b): 
    #print('a.n', a.n)
    #print('b.n', b.n)
    if a.n != b.n: 
        #b=a
        raise Exception("ILLEGAL: non comparable points") 
      
    ret = reduce(lambda x,y: x + pow((a.coords[y]-b.coords[y]), 2),range(a.n),0.0) 
    return math.sqrt(ret) 
 
 
def makeRandomPoint(n, lower, upper):
    p = Point([random.uniform(lower, upper) for i in range(n)]) 
    return p 
    
def CSVtoPRMTRlmbda(bucket, key):
    try:
        response=s3.get_object(Bucket=bucket, Key=key) 
        contents = response['Body'].read()
        lines=contents.splitlines()
        nRows=len(lines) 
        nColumns=len(lines[0])
        Matrix = [[0 for x in range(nColumns)] for y in range(nRows)] 
        i=-1
        for line in lines:
            i=i+1
            #print('i', i)
            parts=line.split(",")
            j=-1
            for part in parts:
                j=j+1
                #print('j', j)
                try:
                    lpart=float(part)
                except:
                    lpart=part
                Matrix[i][j]=lpart
        return Matrix
            
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(keyin, bucketin))
        raise e
        
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