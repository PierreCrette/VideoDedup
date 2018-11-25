#!/usr/bin/env python3

import sys
import os
import subprocess
import fnmatch
import shutil
from os.path import join, getsize
import hashlib
import sqlite3
#import psycopg2
from pprint import pprint

#Declarations
debug = 0
level = 0
creeimages = 0
findimagedupes = 0
fps = 'fps=1/60'
foldervideo= '.'
folderimg = '.'

def BoucleSupp(radical=''):
	if radical != "":
		if radical[-1] != "/": radical = radical + "/"
	if debug>2: print ('BoucleSupp(' + radical + ')')
	if os.path.isdir(folderimg + radical):
		for file in os.listdir(folderimg + radical):
			ext = os.path.splitext(file)[1]
			if (debug > 2) and (ext != '.jpg'): print ('ext = ' + ext + ' -> ' + folderimg + radical + file)
			if (ext.upper() == '.MP4') or (ext.upper() == '.AVI') or (ext.upper() == '.MOV') or (ext.upper() == '.M4V') \
				or (ext.upper() == '.VOB') or (ext.upper() == '.MPG') or (ext.upper() == '.MPEG') or (ext.upper() == '.MKV') \
				or (ext.upper() == '.WMV') or (ext.upper() == '.ASF') or (ext.upper() == '.FLV') \
				or (ext.upper() == '.RM') or (ext.upper() == '.OGM') or (ext.upper() == '.M2TS') or (ext.upper() == '.RMVB'):
				if not(os.path.exists(foldervideo + radical + file)):
					print ('fichier a effacer : ' + folderimg + radical + file)
					shutil.rmtree(folderimg + radical + file)
			if os.path.isdir(folderimg + radical + file):
				BoucleSupp(radical + file)

#Generate jpg images files for one source video file
def OneFile(folderv,folderi,file):
	#Initialization
	fvideo = folderv + file
	fimg = folderi + file + '/img%03d.jpg'
	if debug>3: print ('OneFile(' + folderv +', ' + folderi + ', ' + file + ')')
	
	s = 'ffmpeg -loglevel fatal -i "' + fvideo + '" -vf ' + fps + ' "' + fimg + '"'
	#scale=320:-1 
	
	folderi2 = folderi + file
	if debug>3: print (folderi2)
	
	# Controls
	fait = False
	if os.path.exists(folderi2):
		if os.path.exists(folderi2 + '/run.flag'):
			print('   --- Exist but run.flag so remove image folder')
			shutil.rmtree(folderi2)
		else:
			line = 'fps=1/60'			
			if os.path.exists(folderi2 + '/param.txt'):
				with open(folderi2 + '/param.txt') as f:  
					line = f.readline()
					line = line[:-1]
			if line == fps:
				fait = True
				if debug>1: print ('   --- Previously done ' + folderi2)
			else:
				print ('   --- Previously done but upgrade from ' + line + ' to ' + fps)

	if (fait == False) and (creeimages == 1):
		if debug>2: print ('folderi = ' + folderi + ' file = ' + file)
		if not(os.path.exists(folderi)):
			os.mkdir(folderi, mode=0o777)
		if not(os.path.exists(folderi + file + '/')): 
			os.mkdir(folderi + file + '/', mode=0o777)

		#Create a flag for start over	procedure
		f = open(folderi2 + '/run.flag','w')
		f.write(s + '\n')
		f.close

		#Create a file to store parameters
		f = open(folderi2 + '/param.txt','w')
		f.write(fps + '\n')
		f.close
				
		#Call ffmpeg
		print (s)
		p=subprocess.Popen(s, stdout=subprocess.PIPE, shell=True)
		(output, err) = p.communicate()  
		p_status = p.wait()
		
		os.remove(folderi2 + '/run.flag')

# Parse a single folder to call OneFile for source video files and BoucleFichier recursively if it'a a subfolder
def BoucleFichiers(folderv='.',folderi='.',level=1):
	level = level + 1
	spacer = ''
	if debug>1: 
		for i in range(level): spacer=spacer+'  '
		print(spacer + '[ ' + folderv)
	if os.path.isdir(folderv):
		if not(os.path.exists(folderi)):
			os.mkdir(folderi, mode=0o777)
		if folderv[-1] != "/": folderv = folderv + "/"
		if folderi[-1] != "/": folderi = folderi + "/"
		for file in os.listdir(folderv):
			ext = os.path.splitext(file)[1]
			if os.path.isdir(folderv+file):
				BoucleFichiers(folderv+file,folderi+file)
			elif (ext.upper() == '.MP4') or (ext.upper() == '.AVI') or (ext.upper() == '.MOV') or (ext.upper() == '.M4V') \
				or (ext.upper() == '.VOB') or (ext.upper() == '.MPG') or (ext.upper() == '.MPEG') or (ext.upper() == '.MKV') \
				or (ext.upper() == '.WMV') or (ext.upper() == '.ASF') or (ext.upper() == '.FLV') \
				or (ext.upper() == '.RM') or (ext.upper() == '.OGM') or (ext.upper() == '.M2TS') or (ext.upper() == '.RMVB'):
				OneFile(folderv,folderi,file)
			elif not(ext.upper() == '.JPG' or ext.upper() == '.TXT'):
				print (spacer + '  Not match : ' + folderv + file)
	else:
		OneFile(os.path.dirname(folderv)+"/",os.path.basename(folderv))
	if debug>1: 
		spacer = ''
		for i in range(level): spacer=spacer+'  '
		print (spacer + folderv +  ' ]')
	level = level - 1

#main
#Step0: Read arguments and initialize variables
if debug>3: print(sys.argv)
if len(sys.argv)<2:
	print('SYNTAX ERROR: 1parse folderSRC folderimg [-v] [-i] [-d] [-f]')
	print('-v   Verbose mode')
	print('-i   Create images files in folderimg')
	print('-d   Find duplicates')
	print('-f60   fps: take 1 picture each n seconds. Default fps=1/60 ie 1 picture per minute')
	halt
else:
	foldervideo = os.path.normpath(sys.argv[1])
	if foldervideo[-1] != "/": foldervideo = foldervideo + "/"
	folderimg = os.path.normpath(sys.argv[2])
	if folderimg[-1] != "/": folderimg = folderimg + "/"
	for i in sys.argv[3:]:
		if debug>2: print (i[2:-1])
		if i[:2] == '-v': debug = max(debug,2)
		if i[:2] == '-i': creeimages = 1
		if i[:2] == '-d': findimagedupes = 1
		if i[:2] == '-f': fps = "fps=1/" + i[2:]

	print('************************************************************************************')
	print('* 1parse ' + foldervideo + ' ' + folderimg + ' ' + fps + ' BEGIN')
	print('************************************************************************************')

	if debug>1: print ('foldervideo : ', foldervideo)
	if debug>1: print ('folderimg : ', folderimg)
	if debug>1: print ('fps : ', fps)
	if debug>3: print ('nb args : ',len(sys.argv)-1)
	if debug>5: print ('abspath',os.path.abspath(foldervideo + '..'))
	if debug>5: print ('basename',os.path.basename(foldervideo))
	if debug>5: print ('dirname',os.path.dirname(foldervideo))
	if debug>5: print ('split',os.path.split(foldervideo + '1redfox_caylalyonsmiaggg_1080.mp4'))
	if debug>5: print ('splitext',os.path.splitext(foldervideo + '1redfox_caylalyonsmiaggg_1080.mp4'))
	if debug>5: print ('')
	if debug>5: print ('creeimages = ' + str(creeimages))
	if debug>5: print ('findimagedupes = ' + str(findimagedupes))
	if debug>5: print ('debug = ' + str(debug))
	
	#Step 1: Delete obsolete images
	print ('************************************************************************************')
	print (' Step 1: Delete obsolete images')
	print ('************************************************************************************')
	BoucleSupp('')	
	
	#Step 2: Create missing images		
	print ('************************************************************************************')
	print (' Step 2: Create missing images')
	print ('************************************************************************************')
	BoucleFichiers(foldervideo,folderimg,level)
	
print('************************************************************************************')
print('* 1parse ' + foldervideo + ' ' + folderimg + ' DONE')
print('************************************************************************************')

