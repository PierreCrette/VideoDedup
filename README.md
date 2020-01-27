# VIDEO DeDup

Find duplicate videos by content.
Parse a video directory to create one image every n seconds, then identify duplicate images and show possible video duplicates for manual analysis.

Licenced under GPL-3.0 except for ffmpeg and Imagemagick who have their own licences.



# Version 2

Version published the 9th of August 2019 is considered as final. Regular updates since for performance improvement, better results and some bugs.

. 1f_parse scan video source folder, replicate with ffmpeg an image folder, then for each image create a fingerprint. 1f_parse can also detect moved source file and move accordingly image folder avoiding calculation.

. CompareV2 perform the fingerprints comparison and works in memory with constant memory usage. It is folder agnostic and parse fingerprints ordered by source file name. Since 201906 in free Pascal for x3 speed and less memory usage in multi-threading.

. 3j_analyse is folder agnostic by first loading in memory current source and image folder. Then a lot of options and cache mechanism enable to get correct performance analysis. At the end all duplicates are copied in an analysed folder without doing anything on source or image folders.

. To finalise you'll have to look at analysed folder to make your decisions. I'll suggest then to use a binary duplicate folder to ease real deletion.



# Prerequisites

ffmpeg, imagemagick and python3 must be installed on your computer.

They are all included in Ubuntu distro (sudo apt-get install ffmpeg)

You have a /video folder.
You want to work in a /img folder.

This /img folder will include a /img/db to replicate /video structure and store jpeg images.
This /img folder will alse include a /img/ana-not-saved to copy duplicate files for analysis (remove it from your backup plan).
And also an /img/unwanted to remove imgages and pairs of videos from subsequent searchs.

Due to the duration of each steps (1 To takes about 1 day) you are encouraged to have multiples run on different computers connected to the same NAS. Also you may use different parameters.



# 1f_parse.py

Python program that will scan your video files and create for each file a folder image. In this image folder python calls ffmpeg to create 1 jpeg image every n seconds then it calculate fingerprint of each image (max 16x16 pixels = 256 bits, often 16x9) and store them in a file fingerprint.fp. See findimagedupes documentation for algorythm since I copied it.

1parse.py foldersrc folderimg [options]

foldersrc = where your video files are.

folderimg = where your images will be created. MUST include the /db/ path (see above)

-v=2   Verbose mode

-f=60  fps: take 1 picture each n seconds. Default fps=1/60 ie 1 picture per minute is fast but will miss duplicates. -f=10 (1 images every 10 secs) is balanced. -f=2 is slow but will miss nothing.

-p     Parallel run on multiples computers. Will keep running flags on unfinished folders.

-c     Clean previous runs. To launch after a bunch a parallel run. Exclusive of -p.



# CompareV2

Parse the image folder to load fingerprint.fp files in memory and then compare all and store set of duplicates.

CompareV2 folderimg [options]

-v=2         Verbose mode. Default 1

-s=file      File to log result founds. Default = lbl+incremental number

-lbl=label   Label to identify runs with different parameters. Use the same on all sessions/computers to share workload. No special characters since its use for file naming.

-t=n         Threshold for similarity comparison. Default 10. Huge performance impact. 8 or 10 seems correct.

-threads=n   Number of threads to use. Number of threads of your computer - 1 is fine. Performance impact without RAM usage.

-clean       Read all DB files, remove references to old files, remove duplicates, store all in 1 file.

-log=file    Log file




# 3j_analyse.py

Program who scan duplicates, remove some false duplicates (same image 2 times in the same video file), group duplicates by video pairs, and copy duplicates in the /img/ana-not-saved analyse folder.


1f_analyse -c followed by 1f_analyse -p is a good practice.


SYNTAX : 3analyse foldersrc folderimg findimagedupesresult [options]

-v=n           verbosity. Default=1

-threads=n     number of threads to use. Huge RAM usage. Default=2

-t=n           minimum number of similar images to declare a pair of source as duplicate.

-tu=n          similarity of images vs unwanted to declare as unwanted.

-maxdiff=n     restrict results of findimagedupesresult on similarity < maxdiff

-hdq=n         with n=2 fast 28x16, 3 (previous default) 50x40 little crop, 4 84x51 more crop, 5 84x51x3 colors (default)
                 5 is suggested because there is far less false positives. The CPU impact is mitigated by multithreading.

-hdmaxdiff=n   recalculate high def 50x40 similarity keys to filter final resultset.
                 Optimum range to test are : for hdq=1: 5-10, hdq=2: 15-30, hdq=3: 80-120, hdq=4: 150-300, hdq=5: 500-1500

-skiphd        avoid the 2nd control of HD keys. Faster but more false positives.

-out=file      output a new findimagedupesresult file without unwanted images to speed up next runs.

-tmp=file      to change from default /tmp.

-outhd=file    cache file that keep HDdistance between 2 images.

-ctrlref=False Will accept multiple occurence of a source in different sets. Risk of erasing both elements. Performance and storage hit.

-fake          Will not copy source and image files in analyse folder.

-uwfp          Stop after refreshing fingerprint cache of unwanted images.


Other usage to check individual images and challenge the HD algorithm :

-img=file      Source image to test. Temp image will be keep in /tmp folder.

usage :

3j_analyse -img=./images/video1.mp4/img0001.jpg -img=./images/video2.mp4/img0009.jpg


Examples :

1st run to clean up the resultset file, erase already known unwanted images and remove results on deleted source files : ./3h_analyse.py foldersrc folderimg resultset -t=5 -tu=7 -maxdiff=10 -out=resultset.v1 -fake

2nd run to fill HD cache with main resultsets and to remove some duplicates (few false positives) : ./3h_analyse.py foldersrc folderimg resultset -t=15 -tu=7 -maxdiff=4 -hdmaxdiff=700 

3rd run to fill HD cache with other resultsets and to remove some duplicates (more false positives). Decrease -t and increase -maxdiff by small increment if your resultset is important : ./3h_analyse.py foldersrc folderimg resultset -t=5 -tu=7 -maxdiff=8 -hdmaxdiff=1000 

Last run to deal with remaining data : ./3h_analyse.py foldersrc folderimg resultset -t=5 -tu=7 -maxdiff=10 -hdmaxdiff=1500 -ctrlref=False
    

# HOW TO

How to setup: read above description to understand then modify videodedup.sh to set your own folders.

The parse/ffmpeg are long (1 Tb = 1 day) so do it by subfolder. Also you can call it twice in parallel with -p to use 100% cpu or better on multiple computers connected to a NAS.

## Initial run

Average precision : 1f_parse -f=60; CompareV2 -t=8; 3f_analyse -t=3 -maxdiff=8 -hdmaxdiff=700

Good precision. Will last multiple days running on multiple computers : 1f_parse -f=10; CompareV2 -t=9; 3f_analyse -t=4 -maxdiff=9 -hdmaxdiff=1000

Very good precision. Very long : 1f_parse -f=2; CompareV2 -t=10; 3f_analyse -t=5 -maxdiff=10 -hdmaxdiff=1000

Insane computation : 1f_parse -f=1; CompareV2 -t=10; 3f_analyse -t=5 -maxdiff=10 -hdmaxdiff=1000

## Maintenance run

Use same parameters than initial run. Be sure to keep .db (contains pair of source already compared) + add new resultset to previous ones.

## Remove duplicate

This is out of the scope of this program.

Just move your certified duplicates to a /video/duplicates folder. Then run any dedup program (based on exact binary) to remove BOTH duplicate : your copy in /video/duplicates and the original.

## Remove false positives

Use case : same generic images present in different videos.
Copy the list of jpeg contained in /db/ana-not-saved into /db/unwanted for each set. The 3j_analyse will discard them.

Use case : 2 video files not duplicate but some images are similar.
Copy the nb_match*.txt from /db/ana-not-saved to /db/unwanted. The 3j_analyse will discard them.

## Speed up 1f_parse

Run it with -p on multiple computers connected to a SAN. Use a less agressive fps (4 is twice faster than 2 and will be 4 times faster for CompareV2 step).

## Speed up CompareV2

Limit accepted difference between images. -t=10 is correct, -t=8 is faster, -t=5 is toll less and will miss duplicates.

## Speed up 3j_analyse

First run will be long, so create a limited resultset for next ones. Put unwanted images in unwanted folder and then use -out option and -fake with a permissive -maxdiff equal to -t of CompareV2. The result will be an out file purged of unwanted images so you can empty unwanted folder to speed things. See example 1 above.

HD comparison is long but computed HDfingerprints are store in a cache. The -hdmaxdiff have no impact on this but -maxdiff have. So run a first HD computation with a small -maxdiff (e.g. 4) to find unwanted images and then limit subsequent searchs.



# UNDERSTAND

The 1parse and compare have a recover procedure based on .run files /db/folder/video.run. It will redo incomplete videos or changed in fps parameter.
Between 2 runs (be sure no computer is still working), you can clean up the .run flags : find /folderimg/db -name *.run -exec rm {} \;

To go deeper you can modify python and pascal programs :
1parse.py contains the list of video formats to select. You can add more.
Also .jpg and .txt are removed from 'not match' error. Add your owns.

