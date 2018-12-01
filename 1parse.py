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
import time

#Declarations
debug = 0
level = 0
creeimages = 0
findimagedupes = 0
fps = 'fps=1/60'
parallel = 0
foldervideo= '.'
folderimg = '.'
txtgreen = '\033[0;32m'
txtnocolor = '\033[0m'

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
			if parallel == 0:
				print('   --- Exist but run.flag so remove image folder')
				shutil.rmtree(folderi2)
			else:
				print('   --- run.flag for ' + folderv + file + ' Skip due to parallel mode ')
				fait = True
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
		print (txtgreen + s + txtnocolor)
		t = time.time()
		p=subprocess.Popen(s, stdout=subprocess.PIPE, shell=True)
		(output, err) = p.communicate()  
		p_status = p.wait()
		dur = time.time() - t
		siz = os.path.getsize(fvideo)/1048576
		print(time.asctime(time.localtime(time.time())) + ' - Duration : ' + str(dur) + ' seconds for ' + str(siz) + ' Mb ' + txtgreen + '@ ' + str(siz/dur*0.0864) + ' Tb/day' + txtnocolor)
		
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
		print('folderv = ' + folderv)
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
	print('SYNTAX ERROR: 1parse folderSRC folderimg [-v] [-i] [-d] [-fnn] [-p]')
	print('-v   Verbose mode')
	print('-i   Create images files in folderimg')
	print('-d   Find duplicates')
	print('-f60   fps: take 1 picture each n seconds. Default fps=1/60 ie 1 picture per minute')
	print('-p   Parallel. Will not process if run flag is set')
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
		if i[:2] == '-f': parallel = 1

	print('************************************************************************************')
	print('* ' + txtgreen + '1parse.py ' + foldervideo + ' ' + folderimg + ' ' + fps + txtnocolor)
	print('************************************************************************************')
	print('Video DeDup : find video duplicates')
	print('Copyright (C) 2018  Pierre Crette')
	print('')
	print('This program is free software: you can redistribute it and/or modify')
	print('it under the terms of the GNU General Public License as published by')
	print('the Free Software Foundation, either version 3 of the License, or')
	print('(at your option) any later version.')
	print('')
	print('This program is distributed in the hope that it will be useful,')
	print('but WITHOUT ANY WARRANTY; without even the implied warranty of')
	print('MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the')
	print('GNU General Public License for more details.')
	print('')
	print('You should have received a copy of the GNU General Public License')
	print('along with this program.  If not, see <http://www.gnu.org/licenses/>.')
	print('')

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
	print (' Step 1: Delete obsolete images for ' + foldervideo)
	print ('************************************************************************************')
	BoucleSupp('')	
	
	#Step 2: Create missing images		
	print ('************************************************************************************')
	print (' Step 2: Create missing images for ' + foldervideo)
	print ('************************************************************************************')
	BoucleFichiers(foldervideo,folderimg,level)
	
print('************************************************************************************')
print('* 1parse ' + foldervideo + ' ' + folderimg + ' DONE')
print('************************************************************************************')

