''''
Written by Gregory Reardon - Music and Audio Research Lab (MARL)

This script downloads unprocessed and processed audio files of the bandhub dataset
and writes them out as .flac files after zeropadding the individual tracks such that all 
tracks within one collaboration are of uniform length

Running this code directly performs the downloading

Please use the associated script DownloadAudioBatch.sh to run this script
''''
# ========================================================================================
import argparse
import pandas as pd
import numpy as np
import os
import urllib
import requests
import soundfile as sf 
import errno
import sys
# IMPORTS
# ========================================================================================


def make_dir(directory):
    '''
    This functions creates a new directory for storing files if that directory does not 
    already exist.
    
    Parameters
    ----------
    directory : str
        Name of the directory to be created
        
    Returns
    -------
    None
    '''
    try:
        os.mkdir(directory)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
    pass


def temp_write_url(url, tempPath):
    '''
    This functions writes the .ogg url to a temporary file path which needs to be later 
    be deleted manually by the user
    
    Parameters
    ----------
    url : str
        URL of the .ogg file to be downloaded from the internet
    tempPath : str
        Prepend for the path to the temporary files where the .ogg files are stored
        
    Returns
    -------
    baseFilename : str
        unique filename for a .ogg track grabbed from the URL
    tempOutputFN : str
        full path of the .ogg file 
    '''
    baseFilename = url.rpartition('/')[2] #partition the URL into unique filename
    tempOutputFN = tempPath + '/' + baseFilename #construct an output filename
    try:
        r = requests.get(url)
        with open(tempOutputFN, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
    except requests.exceptions.RequestException as e:
        print(e)
        
    return baseFilename, tempOutputFN


def write_file(filename, tempPath, outputPath, maxLength, startTime):
    '''
    This functions reads in a .ogg temporary file and writes out a .flac file after
    zeropadding such that all published tracks in a mix are of uniform length 
    
    Parameters
    ----------
    filename : str
        Unique name of the audio file
    tempPath : str
        Prepend for the path of the temporary .ogg file which needs to be zeropadded and converted
    outputPath : str
        Prepend for the path of the final output .flac file
    maxLength : int
        Maximum length of all published tracks in a mix so be used to generate files with
        uniform length
    startTime : int
        Start time of the track in samples 
        
    Returns
    -------
    None
    '''
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


def download(HDFName, outputPathUnprocessed,outputPathProcessed, tempPath, startIndex, rowLimit, publishedOnly):
    '''
    This functions performs the file downloading after reading in an HDF file and grouping
    the data rows by the songIDs (so that all tracks within a mix are grabbed together).
    These files are then written out as .flac files in two separate directory, one for
    unprocessed raw audio (without audio effects) and a second for processed audio (with)
    audio effects.
        
    Parameters
    ----------
    HDFName : str
        Name of the hdf5 file
    outputPathUnprocessed : str
        Path to the directory where the unprocessed audio is stored
    outputPathProcessed : str
        Path to the directory where the processed audio is stored
    tempPath : str
        Path to the directory where the .ogg files downloaded when the web are stored
    startIndex : int
        Row index where the downloading will begin
    rowLimit : int
        Total number of rows to be processed
            This cap is soft as all tracks in a mix will be processed even if the rowLimit
            has been reached
    publishedOnly : bool
        If true, only the tracks that are published will be downloaded
        
    Returns
    -------
    None
    '''
    data = pd.read_hdf(HDFName, "bandhub") # read in HDF file

    #dataClean= data.loc[data[columnName].notnull()] #grab unique URLs that are not null

    if publishedOnly is True:
        data = data.loc[data.isPublished == True] #grab published tracks

    data = data[["songId","trackId","audioURL","cleanProcessedAudioURL","startTime","trackDuration"]]
    data = data.iloc[startIndex:startIndex+rowLimit+100]

    groupedData = data.groupby("songId")

    make_dir(outputPathUnprocessed)
    make_dir(outputPathProcessed)
    make_dir(tempPath)
    # try to make new directory, if user has not created it already

    rowCounter = startIndex;
    for name,group in groupedData:
        currRawTracks = group.audioURL
        currProcessedTracks = group.cleanProcessedAudioURL
        startTimes = group.startTime

        validStartTimes = []
        currRawTracksFNs = []
        currProcessedTracksFNs = []

        count = 0;
        maxLength = 0;
        
        #writes out the .ogg files and reads them back in to determine max length of all
        #tracks within a mix
        for url in currRawTracks:
            if url[-4:] == '.ogg':
                baseFilename, tempFilename = temp_write_url(url, tempPath)
                try:
                    audioData,samplerate = sf.read(tempFilename)
                    currLength = max(audioData.shape) + startTimes.iloc[count]
                    currRawTracksFNs.append(baseFilename)
                    validStartTimes.append(startTimes.iloc[count])

                    if currLength > maxLength:
                        maxLength = currLength

                except RuntimeError:
                    print('Unknown data format (unprocessedAudioURL) in row', rowCounter)
            count+=1
            rowCounter +=1
        
        #writes out the .ogg files and reads them back in to determine max length of all
        #tracks within a mix
        for url in currProcessedTracks:
            if pd.notnull(url):
                if url[-4:] == '.ogg':
                    baseFilename, tempFilename = temp_write_url(url,tempPath)
                    try:
                        audioData,samplerate = sf.read(tempFilename)
                        currLength = max(audioData.shape)
                        currProcessedTracksFNs.append(baseFilename)

                        if currLength > maxLength:
                            maxLength = currLength
                    except RuntimeError:
                        print('Unknown data format (processedAudioURL) in row', rowCounter-1)
                #elif currLength != maxLength:
                    #print("Processed Audio Length is not same",url)

        # write out the unprocessed and processed audio as .flac files
        count = 0;
        for fn in currRawTracksFNs:
            write_file(fn, tempPath, outputPathUnprocessed, maxLength, validStartTimes[count])
            count +=1

        if currProcessedTracksFNs:
            for fn in currProcessedTracksFNs:
                write_file(fn, tempPath, outputPathProcessed, maxLength, 0)

        print(rowCounter)
        sys.stdout.flush()
        if rowCounter > rowLimit+startIndex-1:
            print("Final Row Processed", rowCounter - 1)
            break


if __name__ == '__main__':
    '''
    This functions is called if this .py script is run directly and parses the arugments
    and passes them to the functions which perform the dataset download. It will download
    unprocessed audio to the outputPathUnprocessed and processed audio to the
    outputPathProcessed, starting at the startIndex and ending at startIndex + rowLimit.
        
    Parameters (in the accompanying .sh script)
    ----------
    hdfFile : str
        Name of the old hdf5 file whose information is to be appended
    outputPathUnprocessed : str
        Path to the directory where the unprocessed audio will be written
        (ie: /scratch/netID/unprocessedAudio)
    outputPathProcessed : str
        Path to the diretory where the processed audio will be written
        (ie: /scratch/netID/processedAudio)
    tempPath : str
        Path to the directory where the compressed .ogg files will be written
        (ie: /scratch/netID/tempFiles)   
    startIndex : int
        First row of the data to be grabbed (0 is the first row)
    rowLimit : int
        Total number of rows to be processed/downloaded (soft cap)
    publishedTracks : bool
        True if you want only the tracks that are considered published, false it you want
        to download all files in the HDF file. 
        
    Returns
    -------
    None
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("hdfFile", help='Port Number of Mongo Instance', type=str)
    parser.add_argument("outputPathUnprocessed", help = 'Name of folder to download files to (ie: /scratch/netID/TrackAudioU)', type=str)
    parser.add_argument("outputPathProcessed", help = 'Name of folder to download files to (ie: /scratch/netID/TrackAudioP)', type=str)
    parser.add_argument("tempPath", help = 'Name of folder to store temporary files (ie: /scratch/netID/Temp)', type=str)
    parser.add_argument("startIndex", help = 'Index in the dataset to start data download', type=int)   
    parser.add_argument("rowLimit", help = 'The total number of files to be downloaded', type=int)
    parser.add_argument("publishedTracks", help = 'True if you want only published tracks, False if you want the complete set', type=bool)

    args = parser.parse_args()
    hdfName = args.hdfFile
    outputPathUnprocessed = args.outputPathUnprocessed
    outputPathProcessed = args.outputPathProcessed
    tempPath = args.tempPath
    startIndex = args.startIndex
    rowLimit = args.rowLimit
    publishedOnly = args.publishedTracks
    #parse args and store values

    print('Arugments parsed, ready to begin downloading')
    download(hdfName, outputPathUnprocessed, outputPathProcessed, tempPath, startIndex, rowLimit, publishedOnly)