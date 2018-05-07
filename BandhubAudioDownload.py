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


# ========================================================================================
def temp_write_url(url, tempPath):
    baseFilename = url.rpartition('/')[2] #partition the URL into unique filename
    tempOutputFN = tempPath + '/' + baseFilename #construct an output filename

    r = requests.get(url)
    with open(tempOutputFN, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)
    return baseFilename, tempOutputFN

# ========================================================================================
def write_file(filename, tempPath, outputPath, maxLength, startTime):
    tempFN = tempPath + '/' + filename
    outputFN = outputPath + '/' + filename[:-4] + '.flac'
    audioData,samplerate = sf.read(tempFN)
    if samplerate != 44100:
        print("SR is not 44100")

    #print(audioData.shape)
    nChan = audioData.ndim
    lengthInSamples = max(audioData.shape)
    #if startTime > 0 :
    
    if nChan > 1:
        ch1 = np.pad(audioData[:,0], ((startTime, maxLength- (startTime+lengthInSamples)),),'constant')
        ch2 = np.pad(audioData[:,1], ((startTime, maxLength- (startTime+lengthInSamples)),),'constant')
        audioData = np.stack((ch1,ch2),axis = 1)
    else:
        audioData = np.pad(audioData, ((startTime, maxLength- (startTime+lengthInSamples)),),'constant')

    #print(audioData.shape)
    sf.write(outputFN, audioData, samplerate)

# ========================================================================================
def download(HDFName, outputPathUnprocessed,outputPathProcessed, tempPath, startIndex, rowLimit, publishedOnly):
    data = pd.read_hdf(HDFName, "bandhub") # read in HDF file

    #dataClean= data.loc[data[columnName].notnull()] #grab unique URLs that are not null

    if publishedOnly is True:
        data = data.loc[data.isPublished == True] #grab published tracks

    data = data[["songId","trackId","audioURL","cleanProcessedAudioURL","startTime","trackDuration"]]
    data = data.iloc[startIndex:]

    groupedData = data.groupby("songId")
    #dataColumn = pd.unique(dataClean[columnName]) #grab only the column of interest

    try:
        os.mkdir(outputPathUnprocessed)
        os.mkdir(outputPathProcessed)
        os.mkdir(tempPath)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
    pass
    # try to make new directory, if user has not created it already

    rowCounter = startIndex;
    for name,group in groupedData:
        currRawTracks = group.audioURL
        currProcessedTracks = group.cleanProcessedAudioURL
        startTimes = group.startTime

        currRawTracksFNs = []
        currProcessedTracksFNs = []

        count = 0;
        maxLength = 0;
        for url in currRawTracks:
            baseFilename, tempFilename = temp_write_url(url, tempPath)
            audioData,samplerate = sf.read(tempFilename)
            currLength = max(audioData.shape) + startTimes.iloc[count]
            currRawTracksFNs.append(baseFilename)

            if currLength > maxLength:
                maxLength = currLength

            count+=1
            rowCounter +=1

        for url in currProcessedTracks:
            if pd.notnull(url):
                baseFilename, tempFilename = temp_write_url(url,tempPath)
                audioData,samplerate = sf.read(tempFilename)
                currLength = max(audioData.shape)
                currProcessedTracksFNs.append(baseFilename)

                if currLength > maxLength:
                    maxLenth = currLength
                elif currLength != maxLength:
                    #print("Processed Audio Length is not same",url)

        # write out the files
        count = 0;
        for fn in currRawTracksFNs:
            write_file(fn, tempPath, outputPathUnprocessed, maxLength, startTimes.iloc[count])
            count +=1

        if currProcessedTracksFNs:
            for fn in currProcessedTracksFNs:
                write_file(fn, tempPath, outputPathProcessed, maxLength, 0)

        print(rowCounter)
        if rowCounter > rowLimit:
            print("Final Row Processed", rowCounter )
            break

# MAIN FUNCTION
# ========================================================================================
#if running the file directly perform the following
if __name__ == '__main__':
# ========================================================================================
    parser = argparse.ArgumentParser()
    parser.add_argument("HDFFile", help='Port Number of Mongo Instance', type=str)
    parser.add_argument("OutputPathUnprocessed", help = 'Name of folder to download files to (ie: scratch/netID/TrackAudio)', type=str)
    parser.add_argument("OutputPathProcessed", help = 'Name of folder to download files to (ie: scratch/netID/TrackAudio)', type=str)
    parser.add_argument("TempPath", help = 'Name of folder to store temporary files (ie: scratch/netID/Temp)', type=str)
    parser.add_argument("StartIndex", help = 'Index in the dataset to start data download', type=int)   
    parser.add_argument("RowLimit", help = 'The total number of files to be downloaded', type=int)
    parser.add_argument("PublishedTracks", help = 'True if you want only published tracks, False if you want the complete set', type=bool)

    args = parser.parse_args()
    HDFName = args.HDFFile
    outputPathUnprocessed = args.OutputPathUnprocessed
    outputPathProcessed = args.OutputPathProcessed
    tempPath = args.TempPath
    startIndex = args.StartIndex
    rowLimit = args.RowLimit
    publishedOnly = args.PublishedTracks
    #parse args and store values

    print('Arugments parsed, ready to begin downloading')
    download(HDFName, outputPathUnprocessed, outputPathProcessed, tempPath, startIndex, rowLimit, publishedOnly)
# ========================================================================================