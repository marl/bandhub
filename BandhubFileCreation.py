### Create HDF file of bandhub dataset ###
# Running this code directly will create the dataset. #

import argparse
import pymongo
import pandas as pd
import numpy as np
from bson.objectid import ObjectId
import os
import json


def writeData(mongoPortNum, database, fileName, ind):
#This function grabs all of the data of interest from the dataset and puts it into a pandas Dataframe and stores in HDF file
    client = pymongo.MongoClient('localhost',mongoPortNum) #connection to MongoDB instance
    db = client.get_database(database) #grab database

    #grab collections. These should be named as seen below.
    postsCol = db.get_collection('posts')
    videosCol = db.get_collection('mergedVideos')
    songsCol = db.get_collection('songsStream')
    tracksCol = db.get_collection('tracksStream')

    #open the hdf file for storage. Performed here (in lieu of in )to make sure fileName is valid
    hdf = pd.HDFStore(fileName)

    cols = ['trackId', 'songId', 'masterOwner','trackOwner', 'artist', 'title', 'views', 'instrument', 'contentTags', 'audioURL', 'processedAudioURL', 'startTime', 'trackDuration', 'trackVolume', 'compressorValue', 'panValue', 'echoValue', 'noiseGateValue', 'reverbValue', 'solo', 'trackVideo', 'fromYouTube', 'isFinished', 'mixedAudio','mixedVideo', 'musicBrainzID', 'newMusicBrainzID', 'publicSongCollectionIndex']

    #list to hold data
    max_i = 418465 #hard coded the total number of public songs to iterate through 

    cursor = songsCol.find({'access' : 1}).skip(ind).batch_size(10)
    #grab public song collaborations

    i = 0
    try:
        for songDoc in cursor:
        #iterate through each song
            if (i%10 == 0):
                percent = i / max_i
                print('Creating File: %f%%' % percent)
            #print percentage of total documents iterated through
            
            songId = songDoc['_id']
            #grab songId which is unique for each collaboration

            musicBrainzID = songDoc.get('musicbrainzMetadataId')
            newMusicBrainzID = songDoc.get('newMusicbrainzMetadataId')
            #music brainz IDs

            contentTags = json.dumps(songDoc.get('channels'))
            #encode any content tags are JSON strings

            post = postsCol.find({'objectId' : songId})
            #grab the corresponding post document

            videoDocuments = videosCol.find({'songId': songId})
            #find the corresponding video documents. Note: there are multiple videos docs for the same collaboration
            #as instruments are added and tracks swapped out

            for postDoc in post:
            #iterate through corresponding post document and grab relevant information

                artist = postDoc.get('mb_artist')
                views = songDoc.get('numberOfViews')
                title = postDoc.get('title')
                masterOwner = postDoc.get('owner')

                publishedTracks = postDoc['participantsInfo']['publishedTracks']
                trackList = []
                for track in publishedTracks:
                    trackList.append(str(track['_id']))
                sortedTracks = sorted(trackList)
                #grab the array of published tracks for this collaboration and create a list to hold those tracks
                #the track list will be used to compare against trackIds in the video document to determine which
                #video document holds the final mix

                for videoDocs in videoDocuments:
                    toCompare = []
                    for ids in videoDocs['trackIds']:
                        toCompare.append(str(ids))
                    sortedToCompare = sorted(toCompare)
                    #create list to compare list of published tracks to

                    mixedVideo = None
                    if (sortedToCompare == sortedTracks):
                        mixedVideo = videoDocs['mp4MergedVideoUrl']
                        mixedAudio = videoDocs.get('mp3AudioUrl')
                        break

                if mixedVideo is None:
                    mixedVideo = songDoc.get('mp4MergedVideoUrl')

                collabSettings = postDoc.get('collabSettings')
                if collabSettings is None:
                    isFinished = False
                else:
                    isFinished = collabSettings['finished']
                #set bool for whether collaboration is finished
                #check for new field - 'completed'

                for track in publishedTracks:
                #for each track that is published

                    trackId = track['_id']
                    #grab trackId of published track.

                    trackVolume = None
                    mute = None
                    compressorState = None
                    compressorValue = None
                    echoState = None
                    echoValue = None
                    noiseGateState = None
                    noiseGateValue = None
                    panState = None
                    panValue = None
                    reverbState = None
                    reverbValue = None
                    #eqState = None
                    #eqValue = None
                    solo = None
                    #reset these variables if we move to a new published track

                    trackSettings = songDoc['settings'].get(str(trackId))
                    if trackSettings is None:
                        #print('no track settings for this published track')
                        #pprint.pprint(songDoc)
                        #print('')
                        continue
                        #if no track settings move to next track and flag
                    audioChannel = trackSettings.get('audioChannels')
                    #to be used to grab track settings


                    ### AUDIO EFFECTS SETTINGS ###
                    #conflicting values of some effects (including track volume)
                    #inside and outside of audioChannels.

                    #if the settings are located in settings.audioChannel[0] grab them there
                    if audioChannel is not None:
                        trackVolume = audioChannel[0].get('volume')
                        mute = audioChannel[0].get('mute')
                        compressorState = audioChannel[0].get('compressorState')
                        compressorValue = audioChannel[0].get('compressorValue')
                        echoState = audioChannel[0].get('echoState')
                        echoValue = audioChannel[0].get('echoValue')
                        noiseGateState = audioChannel[0].get('noiseGateState')
                        noiseGateValue = audioChannel[0].get('noiseGateValue')
                        panState = audioChannel[0].get('panState')
                        panValue = audioChannel[0].get('panValue')
                        reverbState = audioChannel[0].get('reverbState')
                        reverbValue = audioChannel[0].get('reverbValue')
                        #eqState = audioChannel[0].get('visualEQState')
                        #eqValue = audioChannel[0].get('visualEQValues')
                        solo = audioChannel[0].get('solo')   

                    #if not then try to grab from settings('field')
                    if trackVolume is None:
                        trackVolume = trackSettings.get('volume')
                    if mute is None:
                        mute = trackSettings.get('mute')
                    #if cannot find track volume, check here

                    if(mute == True):
                        trackVolume = 0
                    elif trackVolume is None:
                        trackVolume = -1

                    #compressor
                    if compressorValue is None:
                        compressorState = trackSettings.get('compressorState')
                        compressorValue = trackSettings.get('compressorValue')            
                    if (compressorState == 0) or (compressorValue is None):
                        compressorValue = 0

                    #echo
                    if echoValue is None:
                        echoState = trackSettings.get('echoState')
                        echoValue = trackSettings.get('echoValue')            
                    if (echoState == 0) or (echoValue is None):
                        echoValue = 0
                    
                    #noise gate?
                    if noiseGateValue is None:
                        noiseGateState = trackSettings.get('noiseGateState')
                        noiseGateValue = trackSettings.get('noiseGateValue')            
                    if (noiseGateState == 0) or (noiseGateValue is None):
                        noiseGateValue = 0
                        
                    #pan
                    if panValue is None:
                        panState = trackSettings.get('panState')
                        panValue = trackSettings.get('panValue')
                    if (panState == 0) or (panValue is None):
                        panValue = 0
                        
                    #reverb
                    if reverbValue is None:
                        reverbState = trackSettings.get('reverbState')
                        reverbValue = trackSettings.get('reverbValue')   
                    if (reverbState == 0) or (reverbValue is None):
                        reverbValue = 0
                    
                    #eq
                    #if eqValue is None:
                    #    eqState = trackSettings.get('visualEQState')
                    #    eqValue = trackSettings.get('visualEQValues')
                    
                    #if (eqState == 0):
                    #    eqValue is None
                    
                    #eqValue = json.dumps(eqValue)
                    #print(eqValue)

                    #is solo'ed. I don't think this is ever true
                    if solo is None:
                        solo = trackSettings.get('solo')  

                    ### END AUDIO EFFECTS SETTINGS ###


                    trackDocument = tracksCol.find({'_id' : trackId})
                    #grab the corresponding track document

                    for trackDoc in trackDocument:        
                    #look through corresponding track document    

                        audioURL = trackDoc['audioChannels'][0]['fileUrl']

                        startTime = trackDoc['startTimeValue']
                        #grab unprocessed audio and its start time

                        processedAudioURL = trackSettings.get('effectsAudioUrl')
                        #if there is a processedAudioURL grab it

                        #if no effectsAudioUrl, try these two locations. Also check to make sure
                        #its not a duplicate of the file in the track stream
                        if processedAudioURL is None:
                            if audioChannel is not None:
                                dummyURL = audioChannel[0].get('audioFileUrl')
                                if dummyURL != audioURL:
                                    processedAudioURL = dummyURL
                        if processedAudioURL is None:
                            dummyURL = trackSettings.get('audioFileUrl')
                            if dummyURL != audioURL:
                                processedAudioURL = dummyURL                

                        trackVideo = trackDoc.get('videoFileUrl')
                        if trackVideo is None:
                            trackVideo = trackDoc.get('sourceVideoURL')
                            if trackVideo is not None:
                                fromYouTube = True
                            else:
                                fromYouTube = False
                        else:
                            fromYouTube = False 
                        #grab video files. Set fromYouTube bool

                        trackOwner = trackDoc['owner']
                        #grab the owner of the track
                        trackDuration = trackDoc.get('durationInSeconds')
                        instrument = trackDoc.get('instrumentAssignedBySongOwner')

                        data = []
                        data.append([str(trackId), str(songId), masterOwner, trackOwner, artist, title, views, instrument, contentTags, audioURL, processedAudioURL, startTime, float(trackDuration), float(trackVolume), float(compressorValue), float(panValue), float(echoValue), float(noiseGateValue), float(reverbValue), solo, trackVideo, fromYouTube, isFinished, mixedAudio, mixedVideo, str(musicBrainzID), str(newMusicBrainzID), i])
                        df = pd.DataFrame(data, columns = cols)
                        hdf.append('bandhub', df, format = 'table', min_itemsize = 250, data_columns = True, compression = 'zlib')
                        #append data to file
            i = i + 1
    except pymongo.errors.AutoReconnect:
        print('AutoReconnect error')
        hdf.close()
    try:
        cursor.next()
    except StopIteration:
        print('Done')
        hdf.close()
    hdf.close()


#if running the file directly perform the following
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("portNum", help='Port Number of Mongo Instance', type=int)
    parser.add_argument("databaseName", help='Name of bandhub dataset in MongoDB', type=str)
    parser.add_argument("outputFileName", help = 'Name of hdf file to be output (ie: bandhub.h5)', type=str)
    parser.add_argument("startIndex", help = 'Index in the songs collection from which to begin file creation', type=int)
    #arguments to parse 

    args = parser.parse_args()
    port = args.portNum
    db = args.databaseName
    outputFN = args.outputFileName
    startIndex = args.startIndex

    writeData(port, db, outputFN, startIndex)
    #parse args and store values
