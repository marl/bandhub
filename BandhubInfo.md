Written by Gregory Reardon
This .txt file contains the documentation for python scripts, batch scripts,  HDF file fields, and miscellaneous observations with respect to the Bandhub dataset.

For more information or clarification with respect to any of the below content, please contact Gregory Reardon at the following email:

gregory.reardon9@gmail.com 

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
	1. Validate the downloaded audio and append information to the HDF file
		a. Also handle the 48000 SR material and correct some zero padding if necessary.
		b. Fields to add - audioSampleRate, unprocessedAudioFilename, processedAudioFilename, songDuration 
	1. YouTube video downloads
	1. Mixed Audio?

## HDF File Fields:
####	The most updated version of the HDF file is located in the /scratch/work/marl/bandhub folder and its titled BandhubDataset.h5. The dataset within the HDF file is entitled "bandhub".
####	The dataset contains the following fields:
*	1. trackId - str
		Unique trackId which can be used to locate a track in the tracksStream of the Bandhub database
*	2. songId - str
		Uniquely identifies a collaboration or "mix" which might include a single track of a collection of tracks
*	3. masterOwner - str
*	4. trackOwner - str
*	5. Artist - str
*	6. Title - str
*	7. subTitle - str
*	8. Views - int
*	9. Instrument - str
*	10. contentTags - str (json)
*	11. audioURL - str
*	12. processedAudioURL - str
*	13. trackVideoURL - str
*	14. startTime - int
*	15. trackDuration - float
*	16. audioSampleRate - int
*	17. fromYouTube - bool
*	18. isFinished - bool
*	19. isPublished - bool
*	20. hasPublishedTracks - bool
*	21. mixedAudioURL - str
*	22. mixedVideoURL - str
*	23. musicBrainzID - str
*	24. newMusicBrainzID - str
*	25. publicSongCollectionIndex - int
*	26. volume1 - float
*	27. volume2 - float
*	28. mute1 - bool
*	29. mute2 - bool
*	30. solo1 - bool
*	31. solo2 - bool
*	32. compressorValue1 - float
*	33. compressorValue2 - float
*	34. panValue1 - float
*	35. panValue2 - float
*	36. echoValue1 - float
*	37. echoValue2 - float
*	38. noiseGateValue1 - float
*	39. noiseGateValue2 - float
*	40. reverbValue1 - float
*	41. reverbValue2 - float
*	42. eqValue1 - str (json)
*	43. eqValue2 - str (json)
	

