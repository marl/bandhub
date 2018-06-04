#### Written by Gregory Reardon
#### This .md file contains the documentation for python scripts, batch scripts,  HDF file fields, and miscellaneous observations with respect to the Bandhub dataset.

#### For more information or clarification with respect to any of the below content, please contact Gregory Reardon at the following email: gregory.reardon9@gmail.com 

# Bandhub Documentation:

## 1. HDF File creation
####	python script: BandhubFileCreation.py
####	batch script: BandhubBatch.sh
#### 	batch script arguments: 
    portNum : int
        Name of the hdf5 file
    databaseName : str
        Name of the mongdb database that contains all of the bandhub collections
    outputFileName : str
        Name of the HDF file to be written out
    startIndex : int
        Start index in the song collection (public songs only) that iterated over to 
        generate the HDF file. There were approximately 420000 "public songs" (aka mixes or collaborations) in the database
    documentLimit : int
        Total number of songs in the song collection to be looked through
####	description: These scripts will incrementally iterate through the Bandhub monogodb database (specifically the songsStream) and generate an HDF file with name outputFileName containing relevant information. This process took a very long time and so multiple copies of BandhubFileCreation.py and BandhubBatch.sh were used to generate partitions of the dataset which were then combined using the CombineDatasetPartitions.ipynb
####	misc: CombineDatasetPartitions.ipynb

## 2. HDF Effects Append
####	python script: BandhubAppend.py
####	batch script: AppendBatch.sh
####    batch script arguments:     
    hdfName : str
        Name of the old hdf5 file whose information is to be appended
    portNum : int
        Port number of the mongo process run in the batch script
    outputFileName : str
        Name of the new hdf5 file to be written
    startIndex : int
        First row of the data to be grabbed (0 is the first row)
    documentLimit : int
        Total number of rows of the old dataset to be processed and written out
####	description: These scripts take the original HDF file created using the scripts in section 1 (BandhubFileCreation.py and BandhubBatch.sh) and appends on extra audio effects information. In the original, some of the audio effects were mixed together. In the version created using these scripts, the audio effects are split into two categories based on where they are located in the songs collection. In the songs collection field "settings," a specific tracks settings can be located using the track ID as a keyword. The audio effects settings are located either within those individual track settings or further nested inside the field audioChannels[0]. These two different settings (which for some settings appear to have different ranges) are denoted as x2 and x1, respectively, where x is the type of track setting (volume, mute, solo, reverb, eq etc. ). The effects appending goes much faster than the previous set of scripts and can be run on the whole dataset or split into partitions and once again combined using the Jupyter notebook script CombineDatasetPartitions.ipynb. In addition, the subTitle field was added and the processedAudioURL field was altered.
####	misc: CombineDatasetPartitions.ipynb

## 3. Audio Download
####	python script: BandhubAudioDownload.py
####	batch script: DownloadAudioBatch.sh
####    batch script arguments: 
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
####	description: These scripts download unprocessed (audioURL) and processed (processedAudioURL).ogg audio files (there is a small subset that are .m4a that were not able to be downloaded using pysoundfile), zeropad all files such that all tracks within a mix are of uniform length, and write out the resulting file as .flac. The raw .ogg files are also downloaded to the tempPath directory and need to be manually deleted by user (rm -r tempPath) after the script has been run. Multiple copies of these scripts were also created and used to download different sections of the dataset simultaneously to reduce the time it took to download.
####	misc:

## 4. Video Download
####	python script: BandhubVideoDownload.py
####	batch script: DownloadVideoBatch.sh
####    batch script arguments:
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
        Name of the new hdf5 file to be written
####	description: These scripts download .mp4 track video (trackVideoURL). This procedure is much faster than the scripts in section 3, so multiple copies were not run. Note that YouTube videos were not downloaded.

## 5. File Transfer
####	batch script: FileTransfer.sh
####	description: This script simply uses the rsync command to copy over files to the /scratch/work/marl/bandhub/ folder. Because the audio was originally downloaded to my scratch folder (/scratch/gjr286/) there was a need to copy over the files. 
####	misc:

## 6. Graveyard
#### Contains old .py and .sh download scripts that can be ignored 

## Not Completed:
	1. Convert HDF from Pandas to H5py
	2. Validate the downloaded audio and append information to the HDF file
		a. Also handle the 48000 SR material and correct some zero padding if necessary.
		b. Fields to add - audioSampleRate, unprocessedAudioFilename, processedAudioFilename, songDuration 
	3. YouTube video downloads
	4. Mixed Audio?

## HDF File Fields:
####	The most updated version of the HDF file is located in the /scratch/work/marl/bandhub folder and its titled BandhubDataset.h5. The dataset within the HDF file is entitled "bandhub".
####	The dataset contains the following fields:
* trackId - str
	* Unique trackId which can be used to locate a track in the tracksStream of the Bandhub database
* songId - str
	* Uniquely identifies a song or "mix" which might include a single track or a collection of tracks
* masterOwner - str
	* The username of the individual who began the mix
* trackOwner - str
	* The username of the individual who created the track
* artist - str
	* The artist of the song
* title - str
	* The title of the song
* subTitle - str
	* Contains some additional information regarding the song, such as lyrics or cover style, or describes additional instruments the masterOwner is interested in adding to the current mix
* views - int
	* How many times the mix has been viewed by others on Bandhub
* instrument - str
	* Track-level instrument label (most are blank, see statistics section below)
* contentTags - str (json)
	* Style/genre single-word annotations (most are blank, see statistics section below)
* audioURL - str
	* Raw unprocessed track URL
* processedAudioURL - str
	* Processed track URL (with added audio effects)
* trackVideoURL - str
	* The track video (which does not include any audio)
* startTime - int
	* Start time of the track in the mix (in samples)
* trackDuration - float
	* Length of the track (in seconds)
* audioSampleRate - int
	* Sample rate (this is currently filled with zeros and should be rewritten in the next HDF version)
* fromYouTube - bool
	* Is the track imported from YouTube?
* isFinished - bool
	* Is the mix marked as completed by the masterOwner (see statistics section below)
* isPublished - bool
	* Is the track part of the final mix (or is a draft track)
* hasPublishedTracks - bool
	* Does the song/mix contain at least one published track?
* mixedAudioURL - str
	* Audio URL of the final mixed audio (most are blank)
* mixedVideoURL - str
	* Video URL (which includes audio) of the final mix
* musicBrainzID - str
	* Initial music brainz ID
* newMusicBrainzID - str
	* Music brainz ID after the server was migrated
* publicSongCollectionIndex - int
	* Index in the mongodb song collection after restricting the find function to public tracks (access : 1) (please see the BandhubFileCreation.py)
* volume1 - float
* volume2 - float
	* Track volumes
* mute1 - bool
* mute2 - bool
	* Is the track muted?
* solo1 - bool
* solo2 - bool
	* Is the track solo'ed?
* compressorValue1 - float
* compressorValue2 - float
	* Track compressors
* panValue1 - float
* panValue2 - float
	* Track pan
* echoValue1 - float
* echoValue2 - float
	* Track echo
* noiseGateValue1 - float
* noiseGateValue2 - float
	* Track noise gate?
* reverbValue1 - float
* reverbValue2 - float
	* Track reverb
* eqValue1 - str (json)
* eqValue2 - str (json)
	* Track EQ Values vary in length, therefore they are encoded as json strings

## Dataset Creation Procedure

## Bandhub Basic Statistics

