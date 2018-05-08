def initialize(mongoPortNum, database):
#Initializes and returns all the database collections.
# ========================================================================================

    client = pymongo.MongoClient('localhost',mongoPortNum) #connection to MongoDB instance
    db = client.get_database(database) #grab database
    
    #grab collections. These should be named as seen below.
    #postsCollection = db.get_collection('posts')
    #videosCollection = db.get_collection('mergedVideos')
    songsCollection = db.get_collection('songsStream')
    #tracksCollection = db.get_collection('tracksStream')

    print('Setup Complete, collections grabbed')
    return songsCollection
# ========================================================================================

def writeData(hdfName, songsCol, fileName, ind, documentLimit):
#Grabs all of the data of interest from the dataset and puts it into a pandas Dataframe and stores in HDF file
# ========================================================================================
	
	currData = pd.read_hdf(hdfName, "bandhub") # read in HDF file

    groupedData = data.groupby("songId")
    
    #dataColumn = pd.unique(dataClean[columnName]) #grab only the column of interest
	
    #open the hdf file for storage.
    hdf = pd.HDFStore(fileName)
    print('HDF opened')

    #define the columns of the h5 file
    cols = ['trackId', 'songId', 'masterOwner','trackOwner', 'artist', 'title', 'views', 'instrument', 'contentTags', 'audioURL', 'processedAudioURL', 'startTime', 'trackDuration', 'trackVolume', 'compressorValue', 'panValue', 'echoValue', 'noiseGateValue', 'reverbValue', 'solo', 'trackVideo', 'fromYouTube', 'isFinished', 'mixedAudio','mixedVideo', 'musicBrainzID', 'newMusicBrainzID', 'publicSongCollectionIndex', vol1, vol2]

    max_i = 418465 #hard coded the total number of public songs to iterate through 

    songsFields = {'musicbrainzMetadataId':1,'newMusicbrainzMetadataId':1, 'channels':1,'mp4MergedVideoUrl':1,'numberOfViews':1, 'settings':1}
    #postsFields = {'mb_artist':1,'title':1,'owner':1,'participantsInfo':1,'collabSettings':1}
    #videoFields = {'mp4MergedVideoUrl':1, 'mp3AudioUrl':1, 'trackIds':1}
    #trackFields = {'audioChannels':1,'startTimeValue':1,'videoFileUrl':1,'sourceVideoURL':1,'owner':1,'durationInSeconds':1,'instrumentAssignedBySongOwner':1}       
    # fields needed in each collection, used for projecting to improve performance
	
	
	for name,group in groupedData:
		songDoc = songsCol.find({'_id' : })
	
	
    cursor = songsCol.find({'access' : 1}, songsFields).skip(ind).batch_size(30).limit(documentLimit)
    #grab public song collaborations. Skip how many indices we want, limit files grabbed at one time to 30.
    #Limit the number of files to be looked through to documentLimit

    i = ind
    try:
        for songDoc in cursor:
        #iterate through each song
            #print('For current song')
            if (i%10 == 0):
                percent = i / max_i
                print('Creating File: %f%%' % percent)
            #print percentage of total documents iterated through

            songId, musicBrainzID, newMusicBrainzID, contentTags, songMixedVideo, views = songInformation(songDoc)
            #grab information from song document

            post = postsCol.find({'objectId' : songId}, postsFields).limit(1)
            #grab the corresponding post document

            #print('For post document')
            for postDoc in post:
            #iterate through corresponding post document and grab relevant information
                
                artist, title, masterOwner, publishedTracks, isFinished = postInformation(postDoc)
                #grab and store post document information

                sortedTracks = createSortedTrackList(publishedTracks)
                #grab the sorted track list

                if not sortedTracks:
                    #print('No sorted Tracks')
                    break
                    #if sorted track list is empty, ie: no track id strings, then break and move to next song

                videoDocuments = videosCol.find({'songId': songId}, videoFields)
                #print('video documents grabbed')

                #find the corresponding video documents. Note: there are multiple videos docs for the same collaboration
                #as instruments are added and tracks are swapped out

                mixedVideo, mixedAudio = videoInformation(videoDocuments, sortedTracks, songMixedVideo)
                #grab the correct mixed video and audio

                #print('For each published track')
                for track in publishedTracks:
                #for each track that is published grab the information from the songs collection

                    trackId = track['_id']
                    trackSettings = songDoc['settings'].get(str(trackId))
                    #grab trackId and settings of published track.

                    if trackSettings is None:
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

                        audioURL, processedAudioURL, trackVideo, startTime, trackOwner, trackDuration, instrument, fromYouTube = trackInformation(trackDoc, trackSettings)

                        #print('Ready to create row of data')
                        data = []  #to hold a row of data 
                        data.append([str(trackId), str(songId), masterOwner, trackOwner, artist, title, views, instrument, contentTags, audioURL, processedAudioURL, startTime, float(trackDuration), float(trackVolume), float(compressorValue), float(panValue), float(echoValue), float(noiseGateValue), float(reverbValue), solo, trackVideo, fromYouTube, isFinished, mixedAudio, mixedVideo, str(musicBrainzID), str(newMusicBrainzID), i])
                        df = pd.DataFrame(data, columns = cols) 
                        #store a row of data and convert to dataframe

                        hdf.append('bandhub', df, format = 'table', min_itemsize = 250, data_columns = True, compression = 'zlib')
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
	parser.add_argument("HDFFile", help='Name of HDF File to be rewritten', type=str)
    parser.add_argument("portNum", help='Port Number of Mongo Instance', type=int)
    parser.add_argument("databaseName", help='Name of bandhub dataset in MongoDB', type=str)
    parser.add_argument("outputFileName", help = 'Name of hdf file to be output (ie: bandhub.h5)', type=str)
    parser.add_argument("startIndex", help = 'Index in the songs collection from which to begin file creation', type=int)
    parser.add_argument("documentLimit", help = 'Total number of songs or collaborations to be looked through', type=int)
    #arguments to parse 

    args = parser.parse_args()
    hdfName = args.HDFFile
    port = args.portNum
    db = args.databaseName
    outputFN = args.outputFileName
    startIndex = args.startIndex
    documentLimit = args.documentLimit
    #parse args and store values
    
    print('Arugments parsed, ready to begin writing')
    songsCollection = initialize(port,db) #call the initialize function
    writeData(HDFFile, songsCollection, outputFN, startIndex, documentLimit) #perform the file creation
# ========================================================================================
