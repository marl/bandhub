'''
Written by Gregory Reardon - Music and Audio Research Lab (MARL)

This function creates an hdf5 dataset containing relevant information contained in the
bandhub mongodb database.

Running this code directly writes out a new HDF file

Please use the associated script FileCreation.sh to run this script
'''
import argparse
import pymongo
import pandas as pd
import numpy as np
from bson.objectid import ObjectId
import os
import json
import time
# IMPORTS


def initialize(mongoPortNum, database):
    '''
    This functions connects to the mongo client and returns all the database collections
    
    Parameters
    ----------
    mongoPortNum : int
        Port number of the mongod process.        
    database : str
        Name of the bandhub mongodb database 
        
    Returns
    -------
    songsCollection : object
        The songsStream collection of the bandhub mongodb database
    postsCollection : object
        The posts collection of the bandhub mongodb database
    videosCollection : object
        The mergedVideos collection of the bandhub mongodb database
    tracksCollection : object
        The tracksStream collection of the bandhub mongodb database

    '''
    client = pymongo.MongoClient('localhost',mongoPortNum) #connection to MongoDB instance
    db = client.get_database(database) #grab database
    
    #grab collections. These should be named as seen below.
    postsCollection = db.get_collection('posts')
    videosCollection = db.get_collection('mergedVideos')
    songsCollection = db.get_collection('songsStream')
    tracksCollection = db.get_collection('tracksStream')

    print('Setup Complete, collections grabbed')
    return songsCollection, postsCollection, videosCollection, tracksCollection


def songInformation(songDocument):
    '''
    This functions grabs the information of interest from the passed in songDocument
    
    Parameters
    ----------
    songDocument : dictionary
        A document from the songsCollection from which information will be grabbed
        
    Returns
    -------
    7 different fields from the songDocument
    '''
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


def postInformation(postDocument):
    '''
    This functions grabs the information of interest from the passed in postDocument
    
    Parameters
    ----------
    postDocuments : dictionary
        A document from the postsCollection from which information will be grabbed
        
    Returns
    -------
    5 different fields from the postsDocument
    '''
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


def videoInformation(videoDocuments, sortedTracks, songMixedVideo):
    '''
    This functions grabs all the mixed video and audio from a set of matching video documents
    Pass in a set of video documents, determine which video is final version by
    matching a sorted version of the published tracks (located in the post document)
    to the track IDs in the video document. If no match is found. the mixed video URL in 
    the song collection is used.

    
    Parameters
    ----------
    videoDocuments : cursor
        Cursor which points to all video documents associated with a specific songID 
    sortedTracks : list
        Sorted list of published track IDs located in the post document (see above function).
        Used for matching to the list of track IDs located in the video document
    songMixedVideo : str
        URL to the mixed video which is located in the song document. Note: The mixed 
        video URL can also be found in the video document.
        
    Returns
    -------
    mixedVideo: str
        URL of the mixed video for a collaboration.
    mixedAudio: str
        URL of the mixed audio for a collaboration
    '''
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


def trackInformation(trackDocument, trackSettings, isPublished):
    '''
    This functions grabs the information of interest from the passed in songDocument.
    
    Parameters
    ----------
    trackDocument : dictionary
        Document from the track collection
    trackSettings : dictionary
        A dictionary containing settings for a specific track which are located in the
        corresponding song document 
    isPublished : 
        
    Returns
    -------
    10 different fields
    '''
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

    #If no effectsAudioUrl, try these two locations. 
    #Also check to make sure its not a duplicate of the URL in the track stream
    if cleanProcessedAudioURL is None:
        if audioChannel is not None:
            dummyURL = audioChannel[0].get('audioFileUrl')
            if dummyURL != audioURL:
                processedAudioURL = dummyURL
    if cleanProcessedAudioURL is None:
        dummyURL = trackSettings.get('audioFileUrl')
        if dummyURL != audioURL:
            processedAudioURL = dummyURL      
    # *** NOTE *** 
    #The processedAudioURL was later removed from the dataset only leaving
    #the cleanProcessedAudioURL, but renamed as processedAudioURL

    trackVideo = trackDocument.get('videoFileUrl')
    trackVideo2 = trackDocument.get('sourceVideoURL')

    if (trackVideo is None) and (trackVideo2 is not None):
        trackVideo = trackVideo2

    if trackVideo2 is not None:
        fromYouTube = True
    else:
        fromYouTube = False
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

    return (audioURL, cleanProcessedAudioURL, processedAudioURL, trackVideo, startTime, 
    trackOwner, trackDuration, audioSampleRate, instrument, fromYouTube)


def createSortedTrackList(publishedTracks):
    '''
    This functions takes a list of track IDs and creates a sorted list for comparison
    with the list of trackIDs in the video document
    
    Parameters
    ----------
    publishedTracks : list of str
        List of published track IDs
        
    Returns
    -------
    sortedTrackList : list of str
        Sorted list of published track ids
    publishedTrackList : list of str
        Unsorted list of publishd track ids (after removing invalid/missing list entries)
    '''
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


def grabAudioEffectsSettings(trackSettings):
    '''
    This functions grabs all of the audio effects settings for a specific track. Track 
    settings are located in the song document inside a dictionary whose keys are the track
    IDs.
    
    NOTE : These audio effects were later removed and replaced by the full set of audio
           effects in the script BandhubAppend.py. Please see that script for the 
           final audio effects fields (and the most recent dataset fields)
              
    Parameters
    ----------
    trackSettings : dictionary
        Dictionary holding the audio effects added to a particular track
        
    Returns
    -------
    8 different audio effects fields
    '''
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


def writeData(songsCol, postsCol, videosCol, tracksCol, fileName, ind, documentLimit):
    '''
    This functions calls the above functions, gathers all relevant information for a given
    track into a single row of data. This row of data is converted to a pandas dataframe
    and then written to the HDF file.
    
    NOTE : Later, it was found that writing to the HDF for each row is extremely inefficient
           and may explain while it took exorbitant amounts of time to write out the 
           original HDF file. Please see BandhubAppend.py for how this script can be
           modified to potentially improve its performance (if one is looking to recreate
           the dataset). In BandhubAppend.py, data is gathered in 200 row chunks and then
           written to the file. 
           
    
    Parameters
    ----------
    songsCol : object?
        Mongodb collection of songsStream
    postsCol : object?
        Mongodb collection of posts
    videosCol : object?
        Mongodb collection of mergedVideos
    tracksCol : object?
        Mongodb collection of tracksStream
    fileName : str
        Name of the HDF file that will be written out
    ind : int
        Song collection index to begin at
    documentLimit : int
        Total number of songs to be looked through
        
    Returns
    -------
    None
    '''
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
    #fields needed in each collection, used for projecting to improve performance

    cursor = songsCol.find({'access' : 1}, songsFields).skip(ind).batch_size(15).limit(documentLimit)
    #grab public song collaborations.
    #1. Skip how many indices we want (ind)
    #2. limit files grabbed from database at one time to 15.
    #3. Limit the number of files to be looked through to documentLimit

    i = ind
    try:
        for songDoc in cursor:
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
            #if no trackIDs for a song then immediately move to next song.

            post = postsCol.find({'objectId' : songId}, postsFields).limit(1)
            #grab the corresponding post document

            for postDoc in post:
            #iterate through corresponding post document and grab relevant information
                
                artist, title, masterOwner, publishedTracks, isFinished = postInformation(postDoc)
                #grab and store post document information        
                
                #find the corresponding video documents. 
                #Note: there are multiple videos docs for the same collaboration as 
                #instruments are added and tracks are swapped out

                if not publishedTracks:
                    print('No published tracks')
                    #print(i)
                    print(postDoc['_id'])
                    print(songId)
                    hasPublishedTracks = False
                    mixedVideo = None
                    mixedAudio = None
                else:
                    hasPublishedTracks = True
                    videoDocuments = videosCol.find({'songId': songId}, videoFields)
                    sortedTrackList, publishedTrackList = createSortedTrackList(publishedTracks)
                    mixedVideo, mixedAudio = videoInformation(videoDocuments, sortedTrackList, songMixedVideo)
                    #create a sorted list of published tracks if there are tracks that are published

                crossHasPublishedTracks = songDoc.get('hasPublishedTracks')
                if crossHasPublishedTracks is not None:
                    if not (hasPublishedTracks == crossHasPublishedTracks):
                        print('hasPublishedTracks does not match crossHasPublishedTracks')
                        print(songId)
                #error check if the field hasPublishedTracks in the songs document matches 
                #what the post document reports

                for track in allTracks:
                #for all tracks grab the information from the songs collection

                    trackId = track

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

                    if trackSettings is None:
                        if isPublished:
                            print('Track settings not found for publishedTrack')
                            print(songId)
                            print(trackId)
                        continue
                    #if no track settings move to next track

                    trackVolume, mute, compressorValue, echoValue, noiseGateValue, panValue, reverbValue, solo = grabAudioEffectsSettings(trackSettings)
                    #grab all the audio effects for a track from the songs collection

                    trackDocument = tracksCol.find({'_id' : trackId}, trackFields).limit(1)
                    #grab the corresponding track document
                    
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

                        data = []  #to hold a row of data 
                        data.append([str(trackId), str(songId), masterOwner, trackOwner, artist, title, views, instrument, contentTags, audioURL, cleanProcessedAudioURL, processedAudioURL, startTime, float(trackDuration), int(audioSampleRate), float(trackVolume), float(compressorValue), float(panValue), float(echoValue), float(noiseGateValue), float(reverbValue), solo, trackVideo, fromYouTube, isFinished, isPublished, hasPublishedTracks, mixedAudio, mixedVideo, str(musicBrainzID), str(newMusicBrainzID), i])
                        df = pd.DataFrame(data, columns = cols) 
                        #store a row of data and convert to dataframe

                        hdf.append('bandhub', df, format = 'table', min_itemsize = 500, data_columns = True, compression = 'zlib')
                        print('Data row appended')
                        #append data to file
                        # Note: the min_itemsize is changed to 800 in BandhubAppend.py

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


if __name__ == '__main__':
    '''
    This functions is called when the script is run directly. It will iterate through the
    bandhub song collection (limited to songs that are considered public) starting at 
    startIndex and ending at startIndex+documentLimit and generate an HDF file with the
    fields of interest located in all the bandhub collections (songs, tracks, posts, 
    and mergedVideos)
    
    Parameters (in the accompanying .sh script)
    ----------
    portNum : int
        Name of the hdf5 file
    databaseName : str
        Name of the mongdb database that contains all of the bandhub collections
    outputFileName : str
        Name of the HDF file to be written out
    startIndex : int
        Start index in the song collection (public songs only) that iterated over to 
        generate the HDF file
    documentLimit : int
        Total number of songs in the song collection to be looked through
        
    Returns
    -------
    None
    '''
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
    songsCollection, postsCollection, videosCollection, tracksCollection = initialize(port,db)
    writeData(songsCollection, postsCollection, videosCollection, tracksCollection, outputFN, startIndex, documentLimit)