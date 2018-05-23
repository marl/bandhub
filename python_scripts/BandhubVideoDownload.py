# Written by Gregory Reardon - Music and Audio Research Lab (MARL)
# Download Bandhub Videos (that are not from YouTube)
# Running this code directly will download the videos.
# Please use the associated script DownloadVideoBatch.sh

# ========================================================================================
import argparse
import pandas as pd
import requests
import errno
import os
import sys
# IMPORTS
# ========================================================================================

# ========================================================================================
def make_dir(directory):
    try:
        os.mkdir(directory)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
    pass
    # try to make new directory, if user has not created it already

# ========================================================================================
def write_url(url, outputPath):
    baseFilename = url.rpartition('/')[2] #partition the URL into unique filename
    outputFilename = outputPath + '/' + baseFilename #construct an output filename
    try:
        r = requests.get(url)
        with open(outputFilename, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
    except requests.exceptions.RequestException as e:
        print(e)
        
# ========================================================================================
def download(HDFName, outputPath, startIndex, rowLimit, publishedOnly):
    data = pd.read_hdf(HDFName, 'bandhub') # read in HDF file

    if publishedOnly is True:
        data = data.loc[data.isPublished == True] #grab published tracks

    data = data.iloc[startIndex:startIndex+rowLimit+100]
    dataColumn = data["trackVideoURL"] #grab only the column of interest

    make_dir(outputPath)
    rowCounter = 0
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


# MAIN FUNCTION
# ========================================================================================
# if running the file directly perform the following
if __name__ == '__main__':
# ========================================================================================
    parser = argparse.ArgumentParser()
    parser.add_argument("HDFFile", help='Full path to HDF file', type=str)
    parser.add_argument("OutputPath", help = 'Full path of folder to download video files to', type=str)
    parser.add_argument("StartIndex", help = 'Index in the dataset to start data download', type=int)   
    parser.add_argument("FileLimit", help = 'The total number of files to be downloaded', type=int)
    parser.add_argument("PublishedOnly", help = 'True if you want only published tracks, False if you want the complete set', type=bool)

    args = parser.parse_args()
    HDFName = args.HDFFile
    outputPath = args.OutputPath
    startIndex = args.StartIndex
    fileLimit = args.FileLimit
    publishedOnly = args.PublishedOnly
    #parse args and store values

    print('Arugments parsed, ready to begin downloading')
    download(HDFName, outputPath, startIndex, fileLimit, publishedOnly)
# ========================================================================================