'''
Written by Gregory Reardon - Music and Audio Research Lab (MARL)

This script downloads the track videos of the bandhub dataset

Running this code directly performs the downloading

Please use the associated script DownloadVideo.sh to run this script
'''
import argparse
import pandas as pd
import requests
import errno
import os
import sys
# IMPORTS


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


def write_url(url, outputPath):
    '''
    This functions writes the .ogg url to a temporary file path which needs to be later 
    be deleted manually by the user
    
    Parameters
    ----------
    url : str
        URL of the .mp4 file to be downloaded from the internet
    outputPath : str
        Path where the .mp4 files will be downloaded to.

    Returns
    -------
    None
    '''
    baseFilename = url.rpartition('/')[2] #partition the URL into unique filename
    outputFilename = outputPath + '/' + baseFilename #construct an output filename
    try:
        r = requests.get(url)
        with open(outputFilename, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
    except requests.exceptions.RequestException as e:
        print(e)


def download(hdfName, outputPath, startIndex, rowLimit, publishedOnly):
    '''
    This functions performs the track video downloading after reading in an HDF file.

    Parameters
    ----------
    hdfName : str
        Name of the hdf5 file
    outputPath : str
        Path to the directory where the unprocessed audio is stored
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
    data = pd.read_hdf(hdfName, 'bandhub') # read in HDF file

    if publishedOnly is True:
        data = data.loc[data.isPublished == True] #grab published tracks

    data = data.iloc[startIndex:startIndex+rowLimit+100]
    dataColumn = data["trackVideoURL"] #grab only the column of interest

    make_dir(outputPath)
    rowCounter = startIndex
    for url in dataColumn:
        if pd.notnull(url):
            if url[-4:] == '.mp4':
                write_url(url, outputPath)

        print('Row Processed', rowCounter)
        sys.stdout.flush()
        rowCounter += 1            

        if rowCounter > rowLimit+startIndex - 1:
            break
            
    print("Final Row Processed", rowCounter-1)


if __name__ == '__main__':
    '''
    This functions is called when the script is run directly. It will download the track 
    videos to the outputPath, starting at the startIndex and ending at 
    startIndex + fileLimit.
    
    Parameters (in the accompanying .sh script)
    ----------
    hdfFile : str
        Name of the old hdf5 file whose information is to be appended
    outputPath : str
        Path to the directory where the video files will be stored 
        (ie: /scratch/netID/video)
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
    parser.add_argument("hdfFile", help='Full path to HDF file', type=str)
    parser.add_argument("outputPath", help = 'Full path of folder to download video files to', type=str)
    parser.add_argument("startIndex", help = 'Index in the dataset to start data download', type=int)   
    parser.add_argument("fileLimit", help = 'The total number of files to be downloaded', type=int)
    parser.add_argument("publishedOnly", help = 'True if you want only published tracks, False if you want the complete set', type=bool)

    args = parser.parse_args()
    HDFName = args.hdfFile
    outputPath = args.outputPath
    startIndex = args.startIndex
    fileLimit = args.fileLimit
    publishedOnly = args.publishedOnly
    #parse args and store values

    print('Arugments parsed, ready to begin downloading')
    download(HDFName, outputPath, startIndex, fileLimit, publishedOnly)