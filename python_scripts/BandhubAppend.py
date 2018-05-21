# Written by Gregory Reardon - Music and Audio Research Lab (MARL)
# Append audio effects to current HDF file of bandhub dataset
# Running this code directly perform the appending and write out a new HDF file

# ========================================================================================
import argparse
import pymongo
import pandas as pd
import numpy as np
from bson.objectid import ObjectId
import os
import json
import time
# IMPORTS
# ========================================================================================

def initialize(mongoPortNum):
#Initializes and returns all the database collections.
# ========================================================================================
    client = pymongo.MongoClient('localhost',mongoPortNum) #connection to MongoDB instance
    db = client.get_database('b=bandhub') #grab database
    songsCollection = db.get_collection('songsStream')
    print('Setup Complete, collections grabbed')
    return songsCollection

def check_for_null(string_data):
# Checks if the old data is null using pd.null and sets the string to None for dtype matching
# ========================================================================================
    if pd.isnull(string_data):
        string_data = None
    
    return string_data

def fill_blank_settings():
# Fill default/null settings for HDF store if no track settings are available.
# ========================================================================================
    volume1 = -1
    volume2  = -1
    mute1 = False
    mute2 = False
    solo1 = False
    solo2 = False
    compressorValue1 = -1000
    compressorValue2 = -1000
    echoValue1 = -1000
    echoValue2 = -1000
    noiseGateValue1 = -1000
    noiseGateValue2 = -1000
    panValue1 = -1000
    panValue2 = -1000
    reverbValue1 = -1000
    reverbValue2 = -1000
    eqValue1 = -1000
    eqValue2 = -1000
    
    return (volume1, volume2, mute1, mute2, solo1, solo2, compressorValue1, compressorValue2,
    echoValue1, echoValue2, noiseGateValue1, noiseGateValue2, panValue1, panValue2, 
    reverbValue1, reverbValue2, eqValue1, eqValue2)
    
def grab_audio_effects_settings(trackSettings):
# Grabs all the audio effects settings for a specific track.
# Pass in the the track settings associated with a specific track and information is returned.
# Note: Track settings are located in the songs collection, NOT the track collection
# ========================================================================================
    volume1 = None
    volume2 = None
    mute1 = None
    mute2 = None
    compressorState1 = None
    compressorState2 = None
    compressorValue1 = None
    compressorValue2 = None
    echoState1 = None
    echoState2 = None
    echoValue1 = None
    echoValue2 = None
    noiseGateState1 = None
    noiseGaeState2 = None
    noiseGateValue1 = None
    noiseGateValue2 = None
    panState1 = None
    panState2 = None
    panValue1 = None
    panValue2 = None
    reverbState1 = None
    reverbState2 = None
    reverbValue1 = None
    reverbValue2 = None
    solo1 = None
    solo2 = None
    eqState1 = None
    eqState2 = None
    eqValue1 = None
    eqValue2 = None
    #reset these variables if we move to a new track

    audioChannel = trackSettings.get('audioChannels')
    #to be used to grab track settings

    if audioChannel is not None:
        volume1 = audioChannel[0].get('volume')
        mute1 = audioChannel[0].get('mute')
        solo1 = audioChannel[0].get('solo') 
        
        if mute1 is None:
            mute1 = False
        if solo1 is None:
            solo1 = False
        
        if volume1 is None:
            volume1 = -1
        
        compressorState1 = audioChannel[0].get('compressorState')
        compressorValue1 = audioChannel[0].get('compressorValue')
        if (compressorState1 == 0) or (compressorValue1 is None):
            compressorValue1 = -1000
        
        echoState1 = audioChannel[0].get('echoState')
        echoValue1 = audioChannel[0].get('echoValue')
        if (echoState1 == 0) or (echoValue1 is None):
            echoValue1 = -1000
        
        noiseGateState1 = audioChannel[0].get('noiseGateState')
        noiseGateValue1 = audioChannel[0].get('noiseGateValue')
        if (noiseGateState1 == 0) or (noiseGateValue1 is None):
            noiseGateValue1 = -1000
        
        panState1 = audioChannel[0].get('panState')
        panValue1 = audioChannel[0].get('panValue')
        if (panState1 == 0) or (panValue1 is None):
            panValue1 = -1000
        
        reverbState1 = audioChannel[0].get('reverbState')
        reverbValue1 = audioChannel[0].get('reverbValue')
        if (reverbState1 == 0) or (reverbValue1 is None):
            reverbValue1 = -1000
        
        eqState1 = audioChannel[0].get('visualEQState')
        eqValue1 = audioChannel[0].get('visualEQValues')
        if (eqState1 ==0) or (eqValue1 is None):
            eqValue1 = []
            eqValue1.append(-1000)
    else:
        volume1 = -1
        solo1 = False
        mute1 = False
        compressorValue1 = -1000
        echoValue1 = -1000
        noiseGateValue1 = -1000
        panValue1 = -1000
        reverbValue1 = -1000
        eqValue1 = []
        eqValue1.append(-1000)
    
    volume2 = trackSettings.get('volume')
    mute2 = trackSettings.get('mute')
    solo2 = trackSettings.get('solo') 
    
    if mute2 is None:
        mute2 = False
    if solo2 is None:
        solo2 = False
    
    if volume2 is None:
        volume2 = -1
    
    compressorState2 = trackSettings.get('compressorState')
    compressorValue2 = trackSettings.get('compressorValue')
    if (compressorState2 == 0) or (compressorValue2 is None):
        compressorValue2 = -1000
        
    echoState2 = trackSettings.get('echoState')
    echoValue2 = trackSettings.get('echoValue')
    if (echoState2 == 0) or (echoValue2 is None):
        echoValue2 = -1000
        
    noiseGateState2 = trackSettings.get('noiseGateState')
    noiseGateValue2 = trackSettings.get('noiseGateValue')
    if (noiseGateState2 == 0) or (noiseGateValue2 is None):
        noiseGateValue2 = -1000
        
    panState2 = trackSettings.get('panState')
    panValue2 = trackSettings.get('panValue')    
    if (panState2 == 0) or (panValue2 is None):
        panValue2 = -1000
    
    reverbState2 = trackSettings.get('reverbState')
    reverbValue2 = trackSettings.get('reverbValue')    
    if (reverbState2 == 0) or (reverbValue2 is None):
        reverbValue2 = -1000
         
    eqState2 = trackSettings.get('visualEQState')
    eqValue2 = trackSettings.get('visualEQValues')
    if (eqState2 ==0) or (eqValue2 is None):
        eqValue2 = []
        eqValue2.append(-1000)
         
    if eqValue1 is not None:
        eqValue1 = [ round(elem,2) if elem is not None else elem for elem in eqValue1]
        #eqValue1 = np.around(np.array(eqValue1),3).tolist()
        eqValue1 = json.dumps(eqValue1)
    if eqValue2 is not None:
        eqValue2 = [round(elem,2) if elem is not None else elem for elem in eqValue2]
        #eqValue2 = np.around(np.array(eqValue2),3).tolist()
        eqValue2 = json.dumps(eqValue2)
    
    return (volume1, volume2, mute1, mute2, solo1, solo2, compressorValue1, compressorValue2,
    echoValue1, echoValue2, noiseGateValue1, noiseGateValue2, panValue1, panValue2, 
    reverbValue1, reverbValue2, eqValue1, eqValue2)

def grab_old_data(row):
#Grab data from row of other HDF file
# ========================================================================================
    trackId = row.trackId
    songId = row.songId
    masterOwner = check_for_null(row.masterOwner)
    trackOwner = check_for_null(row.trackOwner)
    artist = check_for_null(row.artist)
    title= check_for_null(row.title)
    views = row.views
    instrument = check_for_null(row.instrument)
    contentTags = row.contentTags
    audioURL = check_for_null(row.audioURL)
    processedAudioURL = check_for_null(row.cleanProcessedAudioURL)
    startTime = row.startTime
    trackDuration = row.trackDuration
    audioSampleRate = row.audioSampleRate
    trackVideo = check_for_null(row.trackVideo)
    fromYouTube = row.fromYouTube
    isFinished = row.isFinished
    isPublished = row.isPublished
    hasPublishedTracks = row.hasPublishedTracks
    mixedAudio = check_for_null(row.mixedAudio)
    mixedVideo = check_for_null(row.mixedVideo)
    musicBrainzID = check_for_null(row.musicBrainzID)
    newMusicBrainzID = check_for_null(row.newMusicBrainzID)
    publicSongCollectionIndex = row.publicSongCollectionIndex
    
    return (trackId, songId, masterOwner, trackOwner, artist, title, views, instrument, 
    contentTags, audioURL, processedAudioURL, startTime, trackDuration, audioSampleRate,
    trackVideo, fromYouTube, isFinished, isPublished, hasPublishedTracks, mixedAudio,
    mixedVideo, musicBrainzID, newMusicBrainzID, publicSongCollectionIndex)



def write_data(hdfName, songsCol, fileName, startIdx, documentLimit):
#Grabs all of the data of interest from the dataset and puts it into a pandas Dataframe and stores in HDF file
# ========================================================================================
    currData = pd.read_hdf(hdfName, "bandhub") # Read in HDF file.
    
    currData = currData.iloc[startIndex:startIndex + documentLimit]

    groupedData = currData.groupby("songId")
    
    #Open the hdf file for storage.
    hdf = pd.HDFStore(fileName)
    print('HDF opened')

    #Define the columns of the h5 file.
    cols = ['trackId', 'songId', 'masterOwner','trackOwner', 'artist', 'title', 'subTitle',
    'views', 'instrument', 'contentTags', 'audioURL', 'processedAudioURL','trackVideoURL',
    'startTime','trackDuration', 'audioSampleRate', 'fromYouTube', 'isFinished', 
    'isPublished', 'hasPublishedTracks','mixedAudioURL','mixedVideoURL', 'musicBrainzID', 
    'newMusicBrainzID', 'publicSongCollectionIndex', 'volume1', 'volume2', 'mute1', 'mute2', 
    'solo1', 'solo2','compressorValue1' , 'compressorValue2', 'panValue1', 'panValue2', 
    'echoValue1','echoValue2', 'noiseGateValue1', 'noiseGateValue2', 'reverbValue1', 
    'reverbValue2', 'eqValue1', 'eqValue2']

    songFields = {'settings':1, 'subTitle':1}       
    # Fields needed in the song collection, used for projecting to improve performance
    
    rowCount = startIdx
    for name,group in groupedData:
        songId = pd.unique(group.songId)[0]
        try:
            songDoc = songsCol.find({'_id' : ObjectId(songId)}, songFields)
            trackIds = group.trackId
            for doc in songDoc:
                songSettings = doc.get('settings')
                subTitle = doc.get('subTitle')
                if len(subTitle) > 700:
                    print('subtitle above length 700')
                    subTitle = subTitle[0:700]
                
                trackCount = 0
                for track in trackIds:
                    trackSettings = songSettings.get(track)
                    if trackSettings is not None:
                        (volume1, volume2, mute1, mute2, solo1, solo2, compressorValue1, 
                        compressorValue2, echoValue1, echoValue2, noiseGateValue1, 
                        noiseGateValue2, panValue1, panValue2, reverbValue1, reverbValue2, 
                        eqValue1, eqValue2) = grab_audio_effects_settings(trackSettings)
                    else:
                        (volume1, volume2, mute1, mute2, solo1, solo2, compressorValue1, 
                        compressorValue2, echoValue1, echoValue2, noiseGateValue1, 
                        noiseGateValue2, panValue1, panValue2, reverbValue1, reverbValue2, 
                        eqValue1, eqValue2) = fill_blank_settings()
                        
                    currRow = group.iloc[trackCount]

                    (trackId, songId, masterOwner, trackOwner, artist, title, views, 
                    instrument, contentTags, audioURL, processedAudioURL, startTime, 
                    trackDuration, audioSampleRate, trackVideo, fromYouTube, isFinished,
                    isPublished, hasPublishedTracks, mixedAudio, mixedVideo, musicBrainzID,
                    newMusicBrainzID, publicSongCollectionIndex) = grab_old_data(currRow)
                                        
                    data = []  #to hold a row of data 
                    
                    data.append([str(trackId), str(songId), masterOwner, trackOwner, 
                    artist, title, subTitle, views, instrument, contentTags, audioURL, 
                    processedAudioURL, trackVideo, startTime, float(trackDuration), 
                    int(audioSampleRate),fromYouTube, isFinished, isPublished, hasPublishedTracks, 
                    mixedAudio, mixedVideo, str(musicBrainzID), str(newMusicBrainzID), 
                    publicSongCollectionIndex, float(volume1), float(volume2), mute1, mute2,
                    solo1, solo2, float(compressorValue1), float(compressorValue2), 
                    float(panValue1), float(panValue2), float(echoValue1), float(echoValue2),
                    float(noiseGateValue1), float(noiseGateValue2), float(reverbValue1),
                    float(reverbValue2), eqValue1, eqValue2])
                    
                    trackCount += 1
                    rowCount += 1

                    
                df = pd.DataFrame(data, columns = cols) 
                #store a row of data and convert to dataframe

                hdf.append('bandhub', df, format = 'table', min_itemsize = 800, data_columns = True, compression = 'zlib')
                print('Data rows appended', rowCount-1)
                #append data to file    
                
        except pymongo.errors.AutoReconnect:
            print('AutoReconnect error')
            hdf.close()
            #catch reconnect error and close file

    hdf.close()
    #this line of code shouldn't be reached, but included to be safe.


# MAIN FUNCTION
# ========================================================================================
#if running the file directly perform the following
if __name__ == '__main__':
# ========================================================================================
    parser = argparse.ArgumentParser()
    parser.add_argument("HDFFile", help='Name of HDF File to be rewritten', type=str)
    parser.add_argument("portNum", help='Port Number of Mongo Instance', type=int)
    parser.add_argument("outputFileName", help = 'Name of hdf file to be output (ie: bandhub.h5)', type=str)
    parser.add_argument("startIndex", help = 'Index in the songs collection from which to begin file creation', type=int)
    parser.add_argument("documentLimit", help = 'Total number of rows to be looked through', type=int)
    #arguments to parse 

    args = parser.parse_args()
    hdfName = args.HDFFile
    port = args.portNum
    outputFN = args.outputFileName
    startIndex = args.startIndex
    documentLimit = args.documentLimit
    #parse args and store values
    
    print('Arugments parsed, ready to begin writing')
    songsCollection = initialize(port) #call the initialize function
    write_data(hdfName, songsCollection, outputFN, startIndex, documentLimit) #perform the file creation
# ========================================================================================
