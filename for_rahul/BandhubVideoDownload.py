# Written by Gregory Reardon - Music and Audio Research Lab (MARL)
# Download Dataset
# Running this code directly will download the dataset.

# ========================================================================================
import argparse
import pymongo
import pandas as pd
import numpy as np
import os
import urllib
import requests
import soundfile as sf

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
def Download(HDFName, outputPath, startIndex, fileLimit, publishedOnly):
    data = pd.read_hdf(HDFName, 'bandhub') # read in HDF file

    if publishedOnly is True:
        data = data.loc[data.isPublished == True] #grab published tracks

    data = data.iloc[startIndex:startIndex+fileLimit]
    dataColumn = data["trackVideo"] #grab only the column of interest

    make_dir(outputPath)
    count = 0
    for url in dataColumn:

        if pd.notnull(url):
            filename = url.rpartition('/')[2] #partition the URL into unique filename
            tempOutputFN = outputPath + '/' + filename #construct an output filename

            r = requests.get(url)
            with open(tempOutputFN, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=128):
                    fd.write(chunk)

        count += 1
        print(count)

        #grab the data and write


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
    Download(HDFName, outputPath, startIndex, fileLimit, publishedOnly)
# ========================================================================================