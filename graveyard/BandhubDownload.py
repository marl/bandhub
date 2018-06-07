# Written by Gregory Reardon - Music and Audio Research Lab (MARL)
# Download Dataset
# Running this code directly will download the dataset.

# ========================================================================================
import argparse
import pymongo
import pandas as pd
import numpy as np
from bson.objectid import ObjectId
import os
import json
import os
import urllib
import requests
import soundfile as sf 

# IMPORTS
# ========================================================================================


#UNCOMPRESSED = False

def Download(filename, columnName, outputPath, startIndex, fileLimit):
    data = pd.read_hdf(filename, 'bandhub') # read in HDF file

    dataClean= data.loc[data[columnName].notnull()] #grab unique URLs that are not null

    if publishedOnly is True:
        dataClean = dataClean.loc[dataClean.isPublished == True] #grab published tracks

    dataColumn = pd.unique(dataClean[columnName]) #grab only the column of interest

    try:
        os.mkdir(outputPath)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
    pass
    # try to make new directory, if user has not created it already
    
    for url in dataColumn[startIndex:startIndex+fileLimit]:
        filename = url.rpartition('/')[2] #partition the URL into unique filename
        tempOutputFN = outputPath + '/' + filename #construct an output filename
        
        r = requests.get(url)
        with open(tempOutputFN, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
        #grab the data and write

        #if UNCOMPRESSED:
        #    fullpath = path + '/' + filename
        #    audioData, samplerate = sf.read(fullpath)




# MAIN FUNCTION
# ========================================================================================
#if running the file directly perform the following
if __name__ == '__main__':
# ========================================================================================
    parser = argparse.ArgumentParser()
    parser.add_argument("HDFFile", help='Port Number of Mongo Instance', type=str)
    parser.add_argument("OutputPath", help = 'Name of folder to download files to (ie: bandhub.h5)', type=str)
    parser.add_argument("DataType", help = 'Type of data to be downloaded (ie: Tracks, Videos, MixedAudio, ProcessedAudio,etc.)', type=str)
    parser.add_argument("StartIndex", help = 'Index in the dataset to start data download', type=int)   
    parser.add_argument("FileLimit", help = 'The total number of files to be downloaded', type=int)
    parser.add_argument("PublishedTracks", help = 'True if you want only published tracks, False if you want the complete set', type=bool)


    #arguments to parse 
    #The various types are 'Tracks', 'Videos', 'ProcessedAudio', 'MixedAudio'

    args = parser.parse_args()
    HDFName = args.HDFFile
    outputPath = args.OutputPath
    dataType = args.DataType
    startIndex = args.StartIndex
    fileLimit = args.FileLimit
    publishedOnly = args.PublishedTracks
    #parse args and store values

    if dataType == "Tracks":
        columnName = "audioURL"
    elif dataType == "Videos":
        columnName = "trackVideo"
    elif dataType == "ProcessedAudio":
        columnName = "cleanProcessedAudioURL"
    elif dataType == "MixedAudio":
        columnName = "mixedVideo"
        #mixed audio is created by stripping the mixed video

    print('Arugments parsed, ready to begin downloading')
    Download(HDFName, columnName, outputPath, startIndex, fileLimit)
    #songsCollection, postsCollection, videosCollection, tracksCollection = initialize(port,db) #call the initialize function
    #writeData(songsCollection, postsCollection, videosCollection, tracksCollection, outputFN, startIndex, documentLimit) #perform the file creation
# ========================================================================================