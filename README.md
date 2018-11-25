# VideoDedup
Parse a video directory to create one image every 60sec with ffmpeg, then identify duplicate images with findimagedupes and ease the result analysis.



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

The parse/ffmpeg are long (1 Tb = 1 day) so do it by subfolder. Also you can call it twice in parallel on 2 different subfolders to use 100% cpu.

Limit the resultset then wider your search. 
--threshold=98 in findimagedupes is better then default 90% for 1st runs
default -f60 is enough in most cases and 4 times faster then -f15 (1 image every 15 seconds)
2 parameter on 2analyse.py tells to discard duplicates with less than 2 similar images

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

The 1parse have a recover procedure based on .txt files stored in /db/folder/video/ folders. It will redo incomplete videos or changed frequency ones.
If you manualy (or with rmimg.sh) remove some images they will be discard from following findimagedupes.


