#### Written by Gregory Reardon
#### This .txt file contains the documentation for python scripts, batch scripts,  HDF file fields, and miscellaneous observations with respect to the Bandhub dataset.

#### For more information or clarification with respect to any of the below content, please contact Gregory Reardon at the following email: gregory.reardon9@gmail.com 

# Bandhub Documentation:

## 1. HDF File creation
####	python script: BandhubFileCreation.py
####	batch script: BandhubBatch.sh
		batch script arguments:
####	description:
####	misc:

## 2. HDF Effects Append
####	python script: BandhubAppend.py
####	batch script: AppendBatch.sh
		batch script arguments: 
####	description:
####	misc:

## 3. Audio Download
####	python script: BandhubAudioDownload.py
####	batch script: DownloadAudioBatch.sh
		batch script arguments:
####	description:
####	misc:

## 4. Video Download
####	python script: BandhubVideoDownload.py
####	batch script: DownloadVideoBatch.sh
		batch script arguments:
####	description:
####	misc:

## 5. File Transfer
####	batch script: FileTransfer.sh
####	description:
####	misc:

## 6. Graveyard
#### Contains 

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
#### Unique trackId which can be used to locate a track in the tracksStream of the Bandhub database
* songId - str
		Uniquely identifies a collaboration or "mix" which might include a single track of a collection of tracks
* masterOwner - str
* trackOwner - str
* Artist - str
* Title - str
* subTitle - str
* views - int
* instrument - str
* contentTags - str (json)
* audioURL - str
* processedAudioURL - str
* trackVideoURL - str
* startTime - int
* trackDuration - float
* audioSampleRate - int
* fromYouTube - bool
* isFinished - bool
* isPublished - bool
* hasPublishedTracks - bool
* mixedAudioURL - str
* mixedVideoURL - str
* musicBrainzID - str
* newMusicBrainzID - str
* publicSongCollectionIndex - int
* volume1 - float
* volume2 - float
* mute1 - bool
* mute2 - bool
* solo1 - bool
* solo2 - bool
* compressorValue1 - float
* compressorValue2 - float
* panValue1 - float
* panValue2 - float
* echoValue1 - float
* echoValue2 - float
* noiseGateValue1 - float
* noiseGateValue2 - float
* reverbValue1 - float
* reverbValue2 - float
* eqValue1 - str (json)
* eqValue2 - str (json)
	

