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
    str=str[lencompany+11:length]
    lenstr=len(str)
    stp=str[0:lenstr-4]
    #print('stp', stp)
   
    
    
    keyos= 'OptimalSignal/' + companyname + '.csv'
    keyout = 'NN/' + companyname + '/JordonSTPKMeans/' 
    keydate='Controllers/DateTime.csv'
    #print("keyout : " , keyout)
    
    
    keyprmtr = 'Controllers/PRMTR.csv' # + keyin[26:length]
    PRMTR=CSVtoPRMTRlmbda(bucketin, keyprmtr)
    nos=PRMTR[1][1]
    #nos=353 # to be commented in production
    crossvalidation=40
    TDNNEpochs=25
    TDNNReset=1
    MatrixField=TickCSVtoMatrixlmbda(bucketin, keyfield,nos)
    #MatrixDate=TickCSVtoMatrixlmbda(bucketin, keydate,0)
    lendata=len(MatrixField)
    #print('MatrixField', MatrixField)
    MatrixOS=TickCSVtoMatrixlmbda(bucketin, keyos,0)
    #print('MatrixOS', MatrixOS)
    lenos=len(MatrixOS)
    MatrixOS=MatrixOS[1:lenos]
    lenos=len(MatrixOS)
    diffos=lendata-lenos
    #print('lendata, lenos', lendata, lenos)
    tdnndepth=12
    #print("field : " , fi)
    
#K-Means starts here
    ifield=42
    data=prepkmeansdata(bucketin,keyfield,ifield,tdnndepth, nos)
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
    
    j=1
    for i,c in enumerate(clusters):
        if (j==0):
            cp =c.centroid
            MatrixToCSVWritelmbda(bucketout, cp,keyout,i, tdnndepth)
        else:
            p =c.points
            MatrixToCSVWritelmbdapointszip(bucketout, p,keyout,i, nos, tdnndepth)
        #print(' Cluster: ', i, '\t centroid :', cp)
        #print('lencp', cp.n)
        #print('cp1 and cp32', cp.coords[0], cp.coords[131] )
        
     
    

    
#functions start from here    
    
def TickCSVtoMatrixlmbda(bucket, key, nos):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        contents = response['Body'].read()
        lines = contents.splitlines()
        lendata=len(lines)
        while lendata<nos:
            time.sleep(2)
            response = s3.get_object(Bucket=bucket, Key=key)
            contents = response['Body'].read()
            lines = contents.splitlines()
            lendata=len(lines)
            
        return lines
    except Exception as e:
        print(e)
        print(
            'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(
                keyin, bucketin))
        raise e
        




def prepkmeansdata(bucketin,key, ifield,tdnndepth, nos):
    keylen=len(key)
    keystart=key[0:keylen-12]
    #print('keystart', keystart)
    pat=[]
    for fi in range(ifield):
        fi=fi+1
        if (fi<=21): start='/NEARST' + str(fi)
        if (fi>21): start='/MIDST' + str(fi-21)
        keyfield=keystart + start + '.csv'
        #print('keyfield', keyfield)
        TrainingData=(TickCSVtoMatrixlmbda(bucketin, keyfield, nos))
        lendata=len(TrainingData)
        TrainingData=TrainingData[tdnndepth:lendata]
        TrainingData = [float(x) for x in TrainingData]
        
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
    return pat
    
def MatrixToCSVWritelmbda(bucket, PI,key,i, tdnndepth):
    nos=PI.n
    newbody=''
    key=key+ str(i) + '.csv'
    #print('key', key)
    for i in range(nos):
        newbody=newbody + str(PI.coords[i])
        newbody=newbody + '\r\n'
    s3.put_object(Bucket=bucket, Key=key, Body=newbody)
    

         
def MatrixToCSVWritelmbdapointszip(bucket, PI,key,i,nos, tdnndepth):
    #nos=PI.n
    newbody=''
    key=key +'F' + str(i) + '.csv'
    data=[]
    for line in PI:
        parts=line.coords
        #parts=map(str, parts)
        #print('parts', parts)
        data.append(parts)
    #data=zip(*data)
    #data=map(str, data)
    rowdata=len(data)
    coldata=len(data[0])+tdnndepth
    #print('rowdata, coldata', rowdata, coldata)
    for i in range(coldata):
        for j in range(rowdata):
            if (j==0):
                if(i<tdnndepth):newbody=newbody + str(0)
                else:newbody=newbody + str(data[j][i-tdnndepth])
            else:
                if(i<tdnndepth):newbody=newbody + ',' + str(0)
                else:newbody=newbody + ',' + str(data[j][i-tdnndepth])
        newbody=newbody + '\r\n'
     
    s3.put_object(Bucket=bucket, Key=key, Body=newbody)
    
    
         
   
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
            print ("Converged after %s iterations" % loopCounter)
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