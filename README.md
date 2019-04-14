# VIDEO DeDup

Find duplicate videos by content.
Parse a video directory to create one image every 60sec with ffmpeg, then identify duplicate images with findimagedupes and ease the result analysis.

Licenced under GPL-3.0 except for ffmpeg and findimagedupes who have their own licences.



# 0. Version 2

. 1f_parse scan video source folder, replicate with ffmpeg an image folder, then for each image create a fingerprint. 1f_parse can also detect moved source file and move accordingly image folder avoiding calculation.
. 2f_compare perform the fingerprints comparison and works in memory with constant memory usage. It is folder agnostic and parse fingerprints ordered by source file name.
. 3f_analyse is folder agnostic by first loading in memory current source and image folder. Then a lot of options and cache mechanism enable to get correct performance analysis. At the end all duplicates are copied in an analysed folder without doing anything on source or image folders.
. To finalise you'll have to look at analysed folder to make your decisions. I'll suggest then to use a binary duplicate folder to ease real deletion



# 0. Prerequisites

ffmpeg must be installed on your computer.
findimagedupes must be installed on your computer.
python3 must be installed on your computer.
They are both included in Ubuntu distro (sudo apt-get install ffmpeg findimagedupes)



# 1. videodedup.sh

Main program. You have to modify it to set your own options

You have a /video folder.
You want to work in a /img folder.
This /img folder will include a /img/db to replicate /video structure and store jpeg images
This /img folder will alse include a /img/ana-not-saved to copy duplicate files for analysis

Due to the duraction of each steps (1 To takes about 1 day) you are encouraged to have multiples 1parse lines for each subfolders. Also you may use different parameters.



# 2. 1parse.py

Python program that will scan your video files and create for each file a folder image. In this folder image python calls ffmpeg to create 1 jpeg image every n seconds.
1parse.py foldersrc folderimg [-v] [-i] [-f]
foldersrc = where your video files are.
folderimg = where your images will be created. MUST include the /db/ path (see above)
-v     Verbose mode
-i     Create images files in folderimg
-f60   fps: take 1 picture each n seconds. Default fps=1/60 ie 1 picture per minute



# 3. findimagedupes

See man page. You can reuse parameters in videodedup.sh. Only change folders.

template.sh is the commands to use with each set found by findimagedupes in order to create a text file with duplicates easier to exploit.



# 4. 2analyse.py

Python program who scan duplicates, remove some false duplicates (same image 2 times in the same video file), group duplicates (for duplicate 1 hour video findimagedupes will found 60 duplicates), and copy duplicates in the /img/ana-not-saved analyse folder.



# HOW TO

How to setup: read above description to understand then modify videodedup.sh to set your own folders.

The parse/ffmpeg are long (1 Tb = 1 day) so do it by subfolder. Also you can call it twice in parallel with -p to use 100% cpu or better on multiple computers connected to a NAS.

Limit the resultset then wider your search. 
--threshold=98 in findimagedupes is better than default 90% for 1st runs
default -f60 is enough in some cases and 4 times faster then -f15 (1 image every 15 seconds). findimagedupes time increase with square of fps. ie -f15 will take 16 times (4x4) than -f60.
Numeric parameter (2 in provided script) on 2analyse.py tells to discard duplicates with less than 2 similar images. Increase it to dicrease false positives.

To go deeper you can modify python programs :
1parse.py contains the list of video formats to select. You can add more.
Also .jpg and .txt are removed from 'not match' error. Add your owns.

Remove false positives
Use case : same generic present in different videos.
Copy the file list of jpeg contained in nb_match.txt into toremove.txt for each set. At the end run rmimg.sh to remove this jpegs.
The 1parse will not create again missing images.

Remove duplicate
This is out of the scope of this program.
Just move your certidied duplicates to a /video/duplicates folder. Then run any dedup program (based on exact binary) to remove BOTH duplicate : your copy in /video/duplicates and the original.



# UNDERSTAND

The 1parse have a recover procedure based on .run files /db/folder/video.run. It will redo incomplete videos or changed in fps parameter.
If you manualy (or with rmimg.sh) remove some images they will be discard from following findimagedupes.
