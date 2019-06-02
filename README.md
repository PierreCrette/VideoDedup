# VIDEO DeDup

Find duplicate videos by content.
Parse a video directory to create one image every 60sec with ffmpeg, then identify duplicate images with findimagedupes and ease the result analysis.

Licenced under GPL-3.0 except for ffmpeg and findimagedupes who have their own licences.



# Version 2

. 1f_parse scan video source folder, replicate with ffmpeg an image folder, then for each image create a fingerprint. 1f_parse can also detect moved source file and move accordingly image folder avoiding calculation.

. compare perform the fingerprints comparison and works in memory with constant memory usage. It is folder agnostic and parse fingerprints ordered by source file name. Since 201906 in free Pascal for x3 speed and less memory usage in multi-threading.

. 3f_analyse is folder agnostic by first loading in memory current source and image folder. Then a lot of options and cache mechanism enable to get correct performance analysis. At the end all duplicates are copied in an analysed folder without doing anything on source or image folders.

. To finalise you'll have to look at analysed folder to make your decisions. I'll suggest then to use a binary duplicate folder to ease real deletion



# Prerequisites

ffmpeg must be installed on your computer.
python3 must be installed on your computer.

They are both included in Ubuntu distro (sudo apt-get install ffmpeg)



# videodedup.sh

Main program. You have to modify it to set your own options.

You have a /video folder.
You want to work in a /img folder.

This /img folder will include a /img/db to replicate /video structure and store jpeg images.
This /img folder will alse include a /img/ana-not-saved to copy duplicate files for analysis.
And also an /img/unwanted to remove imgages and pairs of videos from subsequent searchs.

Due to the duration of each steps (1 To takes about 1 day) you are encouraged to have multiples run on different computers connected to the same NAS. Also you may use different parameters.



# 1f_parse.py

Python program that will scan your video files and create for each file a folder image. In this folder image python calls ffmpeg to create 1 jpeg image every n seconds then it calculate fingerprint of each image (256 bits) and store them in a file fingerprint.fp. See findimagedupes for algorythm.

1parse.py foldersrc folderimg [options]

foldersrc = where your video files are.

folderimg = where your images will be created. MUST include the /db/ path (see above)

-v=2   Verbose mode

-f=60  fps: take 1 picture each n seconds. Default fps=1/60 ie 1 picture per minute

-p     Parallel run on multiples computers. Will keep running flags on unfinished folders.

-c     Clean previous runs. To launch after a bunch a parallel run.

-moved If source file moved will move the image folder. Without -moved previous folder would be erased and then new one created and it's long.



# compare

Parse the image folder to load fingerprint.fp files in memory and then compare all and store set of duplicates.

2f_compare folderimg [options]

-v=2         Verbose mode. Default 1

-s=file      Mandatory. File to log result founds.

-lbl=label   Label to identify runs with different parameters. Use the same on all sessions/computers to share workload. No special characters since its use for file naming.

-t=n         Threshold for similarity comparison. Default 10. Performance impact.

-threads=n   Number of threads to use. Make tests to find better option for your computer. Performance impact.

-mask=n      Not implemented in FPC. To limit the comparison to some images files for each source file. 1/n images are used. Performance impact.

-masksize=n  Not implemented in FPC. Read n images per source then skip based on mask.

-maskmethod= cycle: Not implemented in FPC. read masksize images per source then skip (mask-1)*masksize images, random: if random<1/mask then read masksize images else skip maxsize images.

-checkall    Compare new sources/images against ALL other. By default only against NEXT.    



# 3f_analyse.py

Program who scan duplicates, remove some false duplicates (same image 2 times in the same video file), group duplicates (for duplicate 1 hour video 60 duplicates will be found), and copy duplicates in the /img/ana-not-saved analyse folder.

3f_analyse foldersrc folderimg findimagedupesresult [options]

-v=2           verbosity

-t=5           minimum number of similar images to declare a pair of source as duplicate.

-tu=2          similarity of images vs unwanted to declare as unwanted.

-maxdiff=8     restrict results of findimagedupesresult on similarity < maxdiff. Less or equal to -t parameter of 2f_compare.py.

-hdmaxdiff=50  recalculate high def 57x32 similarity to filter final resultset.

-out=file      output a new findimagedupesresult file without unwanted images to speed up next runs.

-outhd=file    change file used for caching file that keep HDdistance between 2 images.

-ctrlref=False Will accept multiple occurence of a source in different sets. Risk of manual erasing both elements. Performance and storage hit.

-fake          Will not copy source and image files in analyse folder.

    

# HOW TO

How to setup: read above description to understand then modify videodedup.sh to set your own folders.

The parse/ffmpeg are long (1 Tb = 1 day) so do it by subfolder. Also you can call it twice in parallel with -p to use 100% cpu or better on multiple computers connected to a NAS.

## Initial run

Average precision :

1f_parse -f=60; 2f_compare -t=8; 3f_analyse -t=3 -maxdiff=8 -hdmaxdiff=50

Good precision. Will last multiple days running on multiple computers :

1f_parse -f=5; 2f_compare -t=8 -mask=2; 3f_analyse -t=4 -maxdiff=8 -hdmaxdiff=50

Very good precision. Very long :

1f_parse -f=3; 2f_compare -t=8; 3f_analyse -t=5 -maxdiff=8 -hdmaxdiff=50

## Maintenance run

Use same parameters than initial run, but add -checkall to 2f_compare if new files where added in the source folder.

## Remove duplicate

This is out of the scope of this program.

Just move your certified duplicates to a /video/duplicates folder. Then run any dedup program (based on exact binary) to remove BOTH duplicate : your copy in /video/duplicates and the original.

## Remove false positives

Use case : same generic present in different videos.
Copy the list of jpeg contained in /db/ana-not-saved into /db/unwanted for each set. The 3f_analyse will discard them.

Use case : 2 video files not duplicate.
Copy the nb_match*.txt from /db/ana-not-saved to /db/unwanted. The 3f_analyse will discard them.

## Speed up 1f_parse

Run it with -p on multiple computers connected to a SAN. Use a less agressive fps.

## Speed up 2f_compare

Limit accepted difference between images. -t=10 is correct, -t=8 is faster, -t=5 will miss duplicates.

Use mask to not use all data. -mask=2 will use half of fingerprints for a x4 performance gain. x9 with -mask=3.

## Speed up 3f_analyse

First run will be long, so create a limited resultset for next ones. Put unwanted images in unwanted folder and then use -out option and -fake with a permissive -maxdiff equal to -t of 2f_compare. The result will be an out file purged of unwanted images so you can empty unwanted folder to speed things.

HD comparison is long but computed HDfingerprints are store in a cache. The -hdmaxdiff have no impact on this but -maxdiff have. So run a first HD computation with a small -maxdiff (e.g. 4) to find unwanted images and then limit subsequent searchs.



# UNDERSTAND

The 1parse and compare have a recover procedure based on .run files /db/folder/video.run. It will redo incomplete videos or changed in fps parameter.

To go deeper you can modify python programs :
1parse.py contains the list of video formats to select. You can add more.
Also .jpg and .txt are removed from 'not match' error. Add your owns.

