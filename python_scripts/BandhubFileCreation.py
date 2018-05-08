# Written by Gregory Reardon - Music and Audio Research Lab (MARL)
# Create HDF file of bandhub dataset
# Running this code directly will create the dataset.

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


def initialize(mongoPortNum, database):
#Initializes and returns all the database collections.
# ========================================================================================

    client = pymongo.MongoClient('localhost',mongoPortNum) #connection to MongoDB instance
    db = client.get_database(database) #grab database
    
    #grab collections. These should be named as seen below.
    postsCollection = db.get_collection('posts')
    videosCollection = db.get_collection('mergedVideos')
    songsCollection = db.get_collection('songsStream')
    tracksCollection = db.get_collection('tracksStream')

    print('Setup Complete, collections grabbed')
    return songsCollection, postsCollection, videosCollection, tracksCollection
# ========================================================================================




def songInformation(songDocument):
# Grabs all the information for relevant fields from the songs collection.
# Pass in a song document and information is returned.
# ========================================================================================

    songId = songDocument['_id']     
    #grab songId which is unique for each collaboration

    musicBrainzID = songDocument.get('musicbrainzMetadataId')
    newMusicBrainzID = songDocument.get('newMusicbrainzMetadataId')
    #music brainz IDs

    contentTags = json.dumps(songDocument.get('channels'))
    #encode any content tags as JSON strings

    songMixedVideo = songDocument.get('mp4MergedVideoUrl')
    #mixed video link located in the songs document (used in cases where the 'correct' video can't be located)

    views = songDocument.get('numberOfViews')

    allTracks = songDocument.get('trackIDs')

    return songId, musicBrainzID, newMusicBrainzID, contentTags, songMixedVideo, views, allTracks
# ========================================================================================




def postInformation(postDocument):
# Grabs all the information for relevant fields from the posts collection.
# Pass in a post document and information is returned.
# ========================================================================================
    
    artist = postDocument.get('mb_artist')
    title = postDocument.get('title')
    masterOwner = postDocument.get('owner') #masterOwner is owner of individual who starts the collaboration
    publishedTracks = postDocument['participantsInfo']['publishedTracks']
    collabSettings = postDocument.get('collabSettings')
    
    if collabSettings is None:
        isFinished = False
    else:
        isFinished = collabSettings['finished']
    #set bool for whether collaboration is finished. What about completed?

    return artist, title, masterOwner, publishedTracks, isFinished
# ========================================================================================




def videoInformation(videoDocuments, sortedTracks, songMixedVideo):
# Grabs all the mixed video and audio from a set of matching video documents
# Pass in a set of video documents, determine which video is final version by
# matching a sorted version of the published tracks (located in the post collection)
# to the track IDs in the video document.
#   If fail use the mixed video URL in the song collection.
# ========================================================================================
    
    mixedVideo = None
    mixedAudio = None

    for videoDocs in videoDocuments:
        #print('Iterate through the videos')
        toCompare = []
        for ids in videoDocs['trackIds']:
            toCompare.append(str(ids))
        sortedToCompare = sorted(toCompare)
        #create list to compare list of published tracks to

        if (sortedToCompare == sortedTracks):
            mixedVideo = videoDocs['mp4MergedVideoUrl']
            mixedAudio = videoDocs.get('mp3AudioUrl')
            break
            #if the lists are equal grab the mixed video and mixed audio

    if mixedVideo is None:
        mixedVideo = songMixedVideo
        #if no matches in the video collection then just grab from the songs collection

    return mixedVideo, mixedAudio
# ========================================================================================




def trackInformation(trackDocument, trackSettings, isPublished):
# Grabs all the track information.
# Pass in a track document and the tracks settings (from the songs collection) and information is returned.
#   Track settings from the songs collection are used to cross reference the audio URLs
# ========================================================================================
    
    audioURL = trackDocument['audioChannels'][0].get('fileUrl')
    if audioURL is None:
        audioURL = trackDocument.get('audioFileUrl')
        if isPublished:
            print('published track but no audio URL in the expected location')
            print(trackDocument['_id'])
    startTime = trackDocument['startTimeValue']
    #grab unprocessed audio and its start time
    
    cleanProcessedAudioURL = trackSettings.get('effectsAudioUrl')
    processedAudioURL = None
    #if there is a processedAudioURL grab it

    audioChannel = trackSettings.get('audioChannels')

    #if no effectsAudioUrl, try these two locations. Also check to make sure its not a duplicate of the file in the track stream
    if cleanProcessedAudioURL is None:
        if audioChannel is not None:
            dummyURL = audioChannel[0].get('audioFileUrl')
            if dummyURL != audioURL:
                processedAudioURL = dummyURL
    if cleanProcessedAudioURL is None:
        dummyURL = trackSettings.get('audioFileUrl')
        if dummyURL != audioURL:
            processedAudioURL = dummyURL                

    trackVideo = trackDocument.get('videoFileUrl')
    trackVideo2 = trackDocument.get('sourceVideoURL')

    if (trackVideo is None) and (trackVideo2 is not None):
        trackVideo = trackVideo2

    if trackVideo2 is not None:
        fromYouTube = True
    else:
        fromYouTube = False

        #if trackVideo is not None:
        #    fromYouTube = True
        #else:
        #    fromYouTube = False
    #else:
        #fromYouTube = False 
    #grab video files. Set fromYouTube bool

    trackOwner = trackDocument['owner']
    #grab the owner of the track

    audioSampleRate = trackDocument['audioChannels'][0].get('audioSampleRate')
    if audioSampleRate is None:
        audioSampleRate = 0
    #get sample rate

    trackDuration = trackDocument.get('durationInSeconds') #track duration
    
    if trackDuration is None:
        totalFrames = trackDocument['audioChannels'][0].get('audioTotalFrames')
        if totalFrames is not None and audioSampleRate is not 0:
            trackDuration = totalFrames/audioSampleRate
    # if not track duration field them compute the track duration


    instrument = trackDocument.get('instrumentAssignedBySongOwner') #track instrument

    return audioURL, cleanProcessedAudioURL, processedAudioURL, trackVideo, startTime, trackOwner, trackDuration, audioSampleRate, instrument, fromYouTube
# ========================================================================================




def createSortedTrackList(publishedTracks):
# Pass in all published track and create a sorted list of the ids for comparison.
# The sorted list is used in the function videoInformation.
# ========================================================================================
    
    publishedTrackList = []
    for track in publishedTracks:
        try:
            publishedTrackList.append(str(track['_id']))
        except KeyError:
            print('skipped song')
    sortedTrackList = sorted(publishedTrackList)
    #print('Created published tracks array to compare tracks in the videos to')
    #grab the array of published tracks for this collaboration and create a list to hold those tracks
    #the track list will be used to compare against trackIds in the video document to determine which
    #video document holds the final mix.

    return sortedTrackList, publishedTrackList
# ========================================================================================




def grabAudioEffectsSettings(trackSettings):
# Grabs all the audio effects settings for a specific track.
# Pass in the the track settings associated with a specific track and information is returned.
# Note: Track settings are located in the songs collection, NOT the track collection
# ========================================================================================

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
    
    #is solo'ed. I don't think this is ever true, but grab it anyway
    if solo is None:
        solo = trackSettings.get('solo') 

    #eq
    #if eqValue is None:
    #    eqState = trackSettings.get('visualEQState')
    #    eqValue = trackSettings.get('visualEQValues')
    
    #if (eqState == 0):
    #    eqValue is None
    
    #eqValue = json.dumps(eqValue)
    #print(eqValue)

    return trackVolume, mute, compressorValue, echoValue, noiseGateValue, panValue, reverbValue, solo
# ========================================================================================




def writeData(songsCol, postsCol, videosCol, tracksCol, fileName, ind, documentLimit):
#Grabs all of the data of interest from the dataset and puts it into a pandas Dataframe and stores in HDF file
# ========================================================================================

    #open the hdf file for storage.
    hdf = pd.HDFStore(fileName)
    print('HdF opened')

    #define the columns of the h5 file
    cols = ['trackId', 'songId', 'masterOwner','trackOwner', 'artist', 'title', 'views', 'instrument', 'contentTags', 'audioURL', 'cleanProcessedAudioURL', 'processedAudioURL', 'startTime', 'trackDuration', 'audioSampleRate', 'trackVolume', 'compressorValue', 'panValue', 'echoValue', 'noiseGateValue', 'reverbValue', 'solo', 'trackVideo', 'fromYouTube', 'isFinished', 'isPublished', 'hasPublishedTracks', 'mixedAudio','mixedVideo', 'musicBrainzID', 'newMusicBrainzID', 'publicSongCollectionIndex']

    max_i = 418465 #hard coded the total number of public songs to be looked through (only used for print statement) 

    songsFields = {'musicbrainzMetadataId':1,'newMusicbrainzMetadataId':1, 'channels':1,'mp4MergedVideoUrl':1,'numberOfViews':1, 'settings':1, 'trackIDs':1, 'hasPublishedTracks':1}
    postsFields = {'mb_artist':1,'title':1,'owner':1,'participantsInfo':1,'collabSettings':1}
    videoFields = {'mp4MergedVideoUrl':1, 'mp3AudioUrl':1, 'trackIds':1}
    trackFields = {'audioChannels':1, 'audioFileUrl':1, 'startTimeValue':1,'videoFileUrl':1,'sourceVideoURL':1,'owner':1,'durationInSeconds':1,'instrumentAssignedBySongOwner':1}       
    # fields needed in each collection, used for projecting to improve performance
    # 2) add isPublished field
    # 3) add whether song collab has published tracks

    cursor = songsCol.find({'access' : 1}, songsFields).skip(ind).batch_size(15).limit(documentLimit)
    #grab public song collaborations. Skip how many indices we want, limit files grabbed at one time to 30.
    #Limit the number of files to be looked through to 30,000

    i = ind
    try:
        for songDoc in cursor:
        #iterate through each song
            #print('For current song')
            if (i%10 == 0):
                percent = i / max_i
                print('Creating File: %f%%' % percent)
            #print percentage of total documents iterated through

            songId, musicBrainzID, newMusicBrainzID, contentTags, songMixedVideo, views, allTracks = songInformation(songDoc)
            #grab information from song document

            if not allTracks:
                print('No tracks in trackIDs field of song document')
                print(songId)
                i = i+1
                continue
            #if no trackIDs for a song then immediately move to next song. Index needed to move correctly

            post = postsCol.find({'objectId' : songId}, postsFields).limit(1)
            #grab the corresponding post document

            #print('For post document')
            for postDoc in post:
            #iterate through corresponding post document and grab relevant information
                
                artist, title, masterOwner, publishedTracks, isFinished = postInformation(postDoc)
                #grab and store post document information        
                
                #find the corresponding video documents. Note: there are multiple videos docs for the same collaboration
                #as instruments are added and tracks are swapped out

                if not publishedTracks:
                    print('No published tracks')
                    #print(i)
                    print(postDoc['_id'])
                    print(songId)
                    hasPublishedTracks = False
                    mixedVideo = None
                    mixedAudio = None
                    #publishedTracks = songTracks
                    #break
                    #if sorted track list is empty, ie: no track id strings, then break and move to next song
                else:
                    hasPublishedTracks = True
                    videoDocuments = videosCol.find({'songId': songId}, videoFields)
                    #print('video documents grabbed')
                    sortedTrackList, publishedTrackList = createSortedTrackList(publishedTracks)
                    mixedVideo, mixedAudio = videoInformation(videoDocuments, sortedTrackList, songMixedVideo)
                    #create a sorted list of published tracks if there are tracks that are published


                crossHasPublishedTracks = songDoc.get('hasPublishedTracks')
                if crossHasPublishedTracks is not None:
                    if not (hasPublishedTracks == crossHasPublishedTracks):
                        print('hasPublishedTracks does not match crossHasPublishedTracks')
                        print(songId)
                #error check if the field hasPublishedTracks in the songs document matches what the post document reports


                #if not sortedTracks:
                #    print('No sorted tracks')
                #    print('error found')
                #    break
                # shouldn't need this error check if I'm checking for published tracks, TBD

                #grab the correct mixed video and audio
                #this needs to be fixed to handle when there are no sorted tracks. Could just move this above
                #but I need to check if its always the case that no video document exists if tracks arent published

                #print('For each published track')
                for track in allTracks:
                #for all tracks grab the information from the songs collection

                    #trackId = track['_id']
                    trackId = track
                    #this is new

                    isPublished = False
                    if hasPublishedTracks is True:
                        for pubTrackId in publishedTrackList:
                            if str(trackId) == pubTrackId:
                                isPublished = True
                                #print('Published Track Match')
                                break
                    # check to see if published

                    trackSettings = songDoc['settings'].get(str(trackId))
                    #grab trackId and settings of published track.
                    #everything else from here should be the same. 

                    if trackSettings is None:
                        if isPublished:
                            print('Track settings not found for publishedTrack')
                            print(songId)
                            print(trackId)
                        continue
                    #if no track settings move to next track

                    #grab all the audio effects for a track from the songs collection
                    trackVolume, mute, compressorValue, echoValue, noiseGateValue, panValue, reverbValue, solo = grabAudioEffectsSettings(trackSettings)
                    #print('Audio effects grabbed')

                    trackDocument = tracksCol.find({'_id' : trackId}, trackFields).limit(1)
                    #grab the corresponding track document
                    
                    #print('Looking through track document')
                    for trackDoc in trackDocument:        
                    #look through corresponding track document    
                        if trackDoc['audioChannels']:
                            audioURL, cleanProcessedAudioURL, processedAudioURL, trackVideo, startTime, trackOwner, trackDuration, audioSampleRate, instrument, fromYouTube = trackInformation(trackDoc, trackSettings, isPublished)
                        else:
                            if isPublished:
                                print('track is published but no audioChannels in track doc')
                                print(songId)
                                print(trackId)
                            continue
                        #print('Ready to create row of data')
                        data = []  #to hold a row of data 
                        data.append([str(trackId), str(songId), masterOwner, trackOwner, artist, title, views, instrument, contentTags, audioURL, cleanProcessedAudioURL, processedAudioURL, startTime, float(trackDuration), int(audioSampleRate), float(trackVolume), float(compressorValue), float(panValue), float(echoValue), float(noiseGateValue), float(reverbValue), solo, trackVideo, fromYouTube, isFinished, isPublished, hasPublishedTracks, mixedAudio, mixedVideo, str(musicBrainzID), str(newMusicBrainzID), i])
                        df = pd.DataFrame(data, columns = cols) 
                        #store a row of data and convert to dataframe

                        hdf.append('bandhub', df, format = 'table', min_itemsize = 500, data_columns = True, compression = 'zlib')
                        print('Data row appended')
                        #append data to file

            i = i + 1 #increment index counter
    except pymongo.errors.AutoReconnect:
        print('AutoReconnect error')
        hdf.close()
        #catch reconnect error and close file
    
    try:
        cursor.next()
        #check if you can continue to iterate through the cursor
    except StopIteration:
        print('Done')
        hdf.close()
        #catch raised exception for cursor.next and close file
    
    hdf.close()
    #this line of code shouldn't be reached, but included to be safe.



# MAIN FUNCTION
# ========================================================================================
#if running the file directly perform the following
if __name__ == '__main__':
# ========================================================================================
    parser = argparse.ArgumentParser()
    parser.add_argument("portNum", help='Port Number of Mongo Instance', type=int)
    parser.add_argument("databaseName", help='Name of bandhub dataset in MongoDB', type=str)
    parser.add_argument("outputFileName", help = 'Name of hdf file to be output (ie: bandhub.h5)', type=str)
    parser.add_argument("startIndex", help = 'Index in the songs collection from which to begin file creation', type=int)
    parser.add_argument("documentLimit", help = 'Total number of songs or collaborations to be looked through', type=int)
    #arguments to parse 

    args = parser.parse_args()
    port = args.portNum
    db = args.databaseName
    outputFN = args.outputFileName
    startIndex = args.startIndex
    documentLimit = args.documentLimit
    #parse args and store values
    print('Arugments parsed, ready to begin writing')
    songsCollection, postsCollection, videosCollection, tracksCollection = initialize(port,db) #call the initialize function
    writeData(songsCollection, postsCollection, videosCollection, tracksCollection, outputFN, startIndex, documentLimit) #perform the file creation
# ========================================================================================
