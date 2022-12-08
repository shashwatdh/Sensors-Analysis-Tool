import pandas as pd
import numpy as np
import os
import json


class dataset:
    def __init__(self, config_data):
        self.chunkSize = config_data["chunkSize"]
        self.files = config_data["files"]
        #self.cols = config_data["cols"]
        self.isFileBreakEnabled = config_data["isFileBreakEnabled"]
        self.curFileIndex = 0
        self.curChunk = pd.read_csv(filepath_or_buffer=self.files[self.curFileIndex], chunksize=self.chunkSize)
    

    def parseNextDataset(self):
        # enables parsing of new dataset
        if self.curFileIndex < len(self.files)-1:
            self.curFileIndex += 1
            self.curChunk = pd.read_csv(filepath_or_buffer=self.files[self.curFileIndex], 
                                            chunksize=self.chunkSize)
        else:
            raise Exception("FileError: All files have been parsed")


    def fetchNextChunk(self):
        
        # Fetches chunk from a file of size "chunkSize" 
        
        chunk = self.curChunk.get_chunk(self.chunkSize)
        
        if chunk.shape[0] < self.chunkSize:
            if self.curFileIndex < len(self.files)-1:
                
                self.curFileIndex += 1
                self.curChunk = pd.read_csv(filepath_or_buffer=self.files[self.curFileIndex], 
                                            chunksize=self.chunkSize)

                if not self.isFileBreakEnabled:
                    chunk = pd.concat([chunk, self.curChunk.get_chunk(self.chunkSize - chunk.shape[0])])
            else:
                print("All files have been parsed")
        return chunk



class labelExtractor:
    def __init__(self, config_data):
        #self.windowSize = config_data["windowSize"]
        self.datasetChunkSize = config_data["datasetChunkSize"]
        self.files = config_data["files"]
        self.labelCol = config_data["labelCol"]
        self.timestampCol = config_data["timestampCol"]
        self.isFileBreakEnabled = config_data["isFileBreakEnabled"]
        self.datasetObj = dataset({
                                "chunkSize": self.datasetChunkSize, 
                                "files" : self.files, 
                                "cols": [self.timestampCol, self.labelCol],
                                "isFileBreakEnabled": self.isFileBreakEnabled
                            })
        self.datasetChunk = self.datasetObj.fetchNextChunk().iloc[:, [self.timestampCol, self.labelCol]].values
        self.datasetChunkIndex = 0
        self.datasetChunkLastIndex = len(self.datasetChunk) - 1
        self.areAllChunksParsed = False
        

    def fetchLabel(self):
        
        startTS = 0
        endTS = 0
        isCurDatasetParsed = False

        if self.areAllChunksParsed:
            return (-1, -1, "NA", True)

        # ignore the rows having label - "NA"
        while self.datasetChunk[self.datasetChunkIndex][1] == "NA" :
            self.datasetChunkIndex += 1
            
            if self.datasetChunkIndex > self.datasetChunkLastIndex:
                try:
                    self.datasetChunk = self.datasetObj.fetchNextChunk().iloc[:, [self.timestampCol, self.labelCol]].values
                except StopIteration:
                    print("All Chunks are parsed")
                    self.areAllChunksParsed = True
                    #isCurDatasetParsed = True
                    return (-1, -1, "NA", True)
                self.datasetChunkIndex = 0
                self.datasetChunkLastIndex = len(self.datasetChunk) - 1
                

        startTS = self.datasetChunk[self.datasetChunkIndex][0]
        label = self.datasetChunk[self.datasetChunkIndex][1]

        while self.datasetChunk[self.datasetChunkIndex][1] == label:
            
            self.datasetChunkIndex += 1
            
            if self.datasetChunkIndex > self.datasetChunkLastIndex:
                endTS = self.datasetChunk[self.datasetChunkLastIndex][0]
                
                try:
                    self.datasetChunk = self.datasetObj.fetchNextChunk().iloc[:, [self.timestampCol, self.labelCol]].values
                except StopIteration:
                    print("All Chunks are parsed")
                    self.areAllChunksParsed = True
                    return (startTS, endTS, label, True) # isCurDatasetParsed = true

                if (self.datasetChunkLastIndex < self.datasetChunkSize - 1 and
                        self.isFileBreakEnabled):
                    # if file's last chunk is parsed and next dataset isn't related
                    # then send parsed status to sensor node to allow them to directly 
                    # fetch and process next dataset 
                    isCurDatasetParsed = True

                self.datasetChunkIndex = 0
                self.datasetChunkLastIndex = len(self.datasetChunk) - 1
                
                if isCurDatasetParsed:
                    return (startTS, endTS, label, True)

        if self.datasetChunkIndex != 0 :
            endTS = self.datasetChunk[self.datasetChunkIndex - 1][0]
            #return (startTS, endTS, label, False)
        
        return (startTS, endTS, label, False)


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)
