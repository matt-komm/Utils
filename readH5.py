import h5py
import numpy
import math
import random

class GenAccessor:
    @staticmethod
    def createEmptyBatch():
        return {"particles":[],"m_wprime":[],"m_X":[],"m_Y":[]}
    
    @staticmethod
    def fileSize(f):
        return f['particles'].shape[0]
        
    @staticmethod
    def batchSize(batch):
        return sum(map(lambda x: x.shape[0],batch['particles']))
       
    @staticmethod 
    def readFile(batch, f,start,end):
        batch['particles'].append(f['particles'][start:end])
        batch['m_wprime'].append(f['event']['m_wprime'][start:end])
        batch['m_X'].append(f['event']['m_X'][start:end])
        batch['m_Y'].append(f['event']['m_Y'][start:end])
        
    @staticmethod
    def concatenateBatch(batch):
        batch['particles'] = numpy.concatenate(batch['particles'],axis=0)
        batch['m_wprime'] = numpy.concatenate(batch['m_wprime'],axis=0)
        batch['m_X'] = numpy.concatenate(batch['m_X'],axis=0)
        batch['m_Y'] = numpy.concatenate(batch['m_Y'],axis=0)
        

def generate(inputFileList,dataAccessor,batchSize=100,nFiles=2):
    fileIndices = range(len(inputFileList))
    readIndex = 0
    random.shuffle(fileIndices)
    openFiles = []
    
    def totalsize(fileList):
        n = 0
        for f in fileList:
            n+=f['size']-f['index']
        return n
      
    while readIndex==0 or readIndex<len(inputFileList) or totalsize(openFiles)>batchSize:
        while (len(openFiles)<min(len(inputFileList),nFiles) or totalsize(openFiles)<batchSize) and readIndex<(len(inputFileList)):
            #print 'loading ',inputFileList[fileIndices[readIndex]]
            filePath = inputFileList[fileIndices[readIndex]]
            f = h5py.File(filePath)
            openFiles.append({
                'path':filePath,
                'handle':f,
                'index':0,
                'size':dataAccessor.fileSize(f)
            })
            readIndex+=1
    
        if totalsize(openFiles)>batchSize:  
            batch = dataAccessor.createEmptyBatch()
            while (dataAccessor.batchSize(batch)<batchSize):
                currentBatchSize = dataAccessor.batchSize(batch)
                #print '  batch size ',currentBatchSize
                chosenFileIndex = random.randint(0,len(openFiles)-1)
                f = openFiles[chosenFileIndex]
                nread = min([f['size']-f['index'],int(1.*batchSize/nFiles),batchSize-currentBatchSize])
                
                #print '  reading ',f['path'],f['index'],f['index']+nread,f['size']
                
                dataAccessor.readFile(batch,f['handle'],f['index'],f['index']+nread)
                f['index']+=nread
                if f['index']>=f['size']:
                    elem = openFiles.pop(chosenFileIndex)
                    #print 'dequeue ',elem,len(openFiles)
                
            dataAccessor.concatenateBatch(batch)
            yield batch
