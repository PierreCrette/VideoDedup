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
cpttodo = 0
cptdone = 0
debug = 0
level = 0
fps = 'fps=1/60'
parallel = 0
clean = 0
pid = str(time.time())
logfile = '1parse.' + pid + '.log'
foldervideo= '.'
folderimg = '.'
txtgreen = '\033[0;32m'
txtnocolor = '\033[0m'

#log messages to log file and to screen
def log(s='', threshold=1):
	flog.write(s + '\n')
	if debug >= threshold: print(s)

#Step1: remove images with no more source
def BoucleSupp(radical=''):
	if radical != "":
		if radical[-1] != "/": radical = radical + "/"
	log ('BoucleSupp(' + radical + ')', 2)
	if os.path.isdir(folderimg + radical):
		for file in os.listdir(folderimg + radical):
			ext = os.path.splitext(file)[1]
			if ext != '.jpg': log ('ext = ' + ext + ' -> ' + folderimg + radical + file, 2)
			if (ext.upper() == '.MP4') or (ext.upper() == '.AVI') or (ext.upper() == '.MOV') or (ext.upper() == '.M4V') \
				or (ext.upper() == '.VOB') or (ext.upper() == '.MPG') or (ext.upper() == '.MPEG') or (ext.upper() == '.MKV') \
				or (ext.upper() == '.WMV') or (ext.upper() == '.ASF') or (ext.upper() == '.FLV') \
				or (ext.upper() == '.RM') or (ext.upper() == '.OGM') or (ext.upper() == '.M2TS') or (ext.upper() == '.RMVB'):
				if not(os.path.exists(foldervideo + radical + file)):
					log ('fichier a effacer : ' + folderimg + radical + file, 0)
					shutil.rmtree(folderimg + radical + file)
			if os.path.isdir(folderimg + radical + file):
				BoucleSupp(radical + file)

#Count source to do
def BoucleCount(folderv='.', folderi='.', level=1):
	global cpttodo

	level = level + 1
	spacer = ''
	if debug>0: 
		for i in range(level): spacer=spacer+'  '
		log(spacer + '[ ' + folderv, 0)
	if os.path.isdir(folderv):
		if not(os.path.exists(folderi)):
			os.mkdir(folderi, mode=0o777)
		if folderv[-1] != "/": folderv = folderv + "/"
		if folderi[-1] != "/": folderi = folderi + "/"
		for file in os.listdir(folderv):
			ext = os.path.splitext(file)[1]
			if os.path.isdir(folderv+file):
				BoucleCount(folderv+file, folderi+file, level+1)
			elif (ext.upper() == '.MP4') or (ext.upper() == '.AVI') or (ext.upper() == '.MOV') or (ext.upper() == '.M4V') \
				or (ext.upper() == '.VOB') or (ext.upper() == '.MPG') or (ext.upper() == '.MPEG') or (ext.upper() == '.MKV') \
				or (ext.upper() == '.WMV') or (ext.upper() == '.ASF') or (ext.upper() == '.FLV') \
				or (ext.upper() == '.RM') or (ext.upper() == '.OGM') or (ext.upper() == '.M2TS') or (ext.upper() == '.RMVB'):
				cpttodo = cpttodo + 1
			elif not(ext.upper() == '.JPG' or ext.upper() == '.TXT'):
				log (spacer + '  Not match : ' + folderv + file, 2)
	if debug>0: 
		spacer = ''
		for i in range(level): spacer=spacer+'  '
		log (spacer + '  ' + folderv + ' count = ' + str(cpttodo) + ' ]', 0)
	level = level - 1

#Generate jpg images files for one source video file
def OneFile(folderv, folderi, file):
	global cpttodo, cptdone
	
	#Initialization
	fvideo = folderv + file
	fimg = folderi + file + '/img%05d.jpg'
	log ('OneFile(' + folderv +', ' + folderi + ', ' + file + ')', 2)
	
	s = 'ffmpeg -loglevel fatal -i "' + fvideo + '" -vf ' + fps + ' "' + fimg + '"'
	#scale=320:-1 
	
	folderi2 = folderi + file
	log (folderi2, 3)
	
	# Controls
	todo = True
	if os.path.exists(folderi2):
		line = 'fps=1/60'			
		if os.path.exists(folderi2 + '/param.txt'):
			with open(folderi2 + '/param.txt') as f:  
				line = f.readline()
				line = line[:-1]
		if len(line) <= 6:
			log('   --- Param.txt inconsistent', 2)
			line = 'fps=1/999'
		log('Test fps: ' + line[6:] + ' <= ? ' + fps[6:], 2)
		if int(line[6:]) <= int(fps[6:]):
			todo = False
			log ('   --- Previously done ' + folderi2, 1)
		else:
			log (folderi2 + ' previously done but upgrade from ' + line + ' to ' + fps, 0)
	else:
		log (folderi2 + ' to do.', 2)

	#Cleanup based on startover mechanism
	if (clean == 1):
		if os.path.exists(folderi2 + '.run'):
			log('CLEAN due to lock: ' + folderi2, 0)
			os.remove(folderi2 + '.run')
			if os.path.exists(folderi2):
				shutil.rmtree(folderi2)
		if todo:
			if os.path.exists(folderi2):
				log('CLEAN due to parameters: ' + folderi2, 0)
				shutil.rmtree(folderi2)		

	#Lock mechanism for startover procedure and parral mode
	if todo:
		if (parallel == 0):
			if os.path.exists(folderi2 + '.run'):
				log('   --- Exist but .run flag so remove image folder', 0)
				if os.path.exists(folderi2): 
					shutil.rmtree(folderi2)
				os.remove(folderi2 + '.run')
			log('set ' + folderi2 + '.run flag',2)
			f = open(folderi2 + '.run','w')
			f.write(pid + '\n')
			f.close
		if (parallel == 1):
			if os.path.exists(folderi2 + '.run'):
				todo = False
				log('   --- .run flag for ' + folderv + file + ' Skip due to parallel mode ', 0)
			else:
				log('set ' + folderi2 + '.run flag',2)
				f = open(folderi2 + '.run','w')
				f.write(pid + '\n')
				f.close
				time.sleep(3)
				with open(folderi2 + '.run') as f:  
					line = f.readline()
					line = line[:-1]
				log (line + ' =? ' + pid, 2)
				if line != pid:
					todo = False
					log('   -------------------------------------------------------------------', 0)
					log('   --- Concurent run detected !', 0)
					log('   --- .run flag for ' + folderv + file + ' Skip due to parallel mode ', 0)
					log('   -------------------------------------------------------------------', 0)
				
	# Execute
	if todo :
		if os.path.exists(folderi2):
			shutil.rmtree(folderi2)
		
		if clean == 1:
			cptdone = cptdone - 1
		else:
			log ('Call ffmpeg with folderi = ' + folderi + ' file = ' + file, 2)

			if not(os.path.exists(folderi)):
				os.mkdir(folderi, mode=0o777)
			if not(os.path.exists(folderi + file + '/')): 
				os.mkdir(folderi + file + '/', mode=0o777)

			#Create a file to store parameters
			f = open(folderi2 + '/param.txt','w')
			f.write(fps + '\n')
			f.close
					
			#Call ffmpeg
			log (txtgreen + s + txtnocolor, 0)
			t = time.time()
			p=subprocess.Popen(s, stdout=subprocess.PIPE, shell=True)
			(output, err) = p.communicate()  
			p_status = p.wait()
			dur = time.time() - t
			siz = os.path.getsize(fvideo)/1048576
			log(time.asctime(time.localtime(time.time())) + ' - Duration : ' + str(round(dur,3)) + ' seconds for ' + str(round(siz,0)) + ' Mb ' + txtgreen + '@ ' + str(round(siz/dur*0.0864,2)) + ' Tb/day' + txtnocolor, 0)
		
		os.remove(folderi2 + '.run')

	cptdone = cptdone + 1
	if clean == 1:
		log(str(cptdone) + ' / ' + str(cpttodo) + ' done...', 2)
	else:
		log(str(cptdone) + ' / ' + str(cpttodo) + ' done...', 0)

# Parse a single folder to call OneFile for source video files and BoucleFichier recursively if it'a a subfolder
def BoucleFichiers(folderv='.', folderi='.', level=1):
	level = level + 1
	spacer = ''
	if debug>1: 
		for i in range(level): spacer=spacer+'  '
		log(spacer + '[ ' + folderv, 0)
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
				log (spacer + '  Not match : ' + folderv + file, 0)
	else:
		log('folderv = ' + folderv, 0)
		OneFile(os.path.dirname(folderv)+"/",os.path.basename(folderv))
	if debug>1: 
		spacer = ''
		for i in range(level): spacer=spacer+'  '
		log (spacer + folderv +  ' ]', 0)
	level = level - 1

#main
#Step0: Read arguments and initialize variables
print ('')
print (str(sys.argv))
if len(sys.argv)<2:
	print('SYNTAX ERROR: 1parse folderSRC folderimg [-v] [-i] [-d] [-fnn] [-p]')
	print('-v   Verbose mode')
	print('-f60   fps: take 1 picture each n seconds. Default fps=1/60 ie 1 picture per minute')
	print('-p   Parallel. Will not process if run flag is set')
	print('-c   Clean. Will not execute ffmpeg but will remove unfinished images: run.flag exist or incorrect fps.')
	print('-log=file   Log file')
	halt
else:
	foldervideo = os.path.normpath(sys.argv[1])
	if foldervideo[-1] != "/": foldervideo = foldervideo + "/"
	folderimg = os.path.normpath(sys.argv[2])
	if folderimg[-1] != "/": folderimg = folderimg + "/"
	for i in sys.argv[3:]:
		print (i[2:-1])
		if i[:2] == '-v': debug = max(debug,1)
		if i[:2] == '-f': fps = "fps=1/" + i[2:]
		if i[:2] == '-p': parallel = 1
		if i[:2] == '-c': clean = 1
		if i[:5] == '-log=': logfile = i[5:]

	flog = open(logfile,'w')
		
	log('************************************************************************************', 0)
	log('* ' + txtgreen + '1parse.py ' + foldervideo + ' ' + folderimg + ' ' + fps + txtnocolor, 0)
	log('************************************************************************************', 0)
	log('Video DeDup : find video duplicates', 0)
	log('Copyright (C) 2018  Pierre Crette', 0)
	log('', 0)
	log('This program is free software: you can redistribute it and/or modify', 0)
	log('it under the terms of the GNU General Public License as published by', 0)
	log('the Free Software Foundation, either version 3 of the License, or', 0)
	log('(at your option) any later version.', 0)
	log('', 0)
	log('This program is distributed in the hope that it will be useful,', 0)
	log('but WITHOUT ANY WARRANTY; without even the implied warranty of', 0)
	log('MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the', 0)
	log('GNU General Public License for more details.', 0)
	log('', 0)
	log('You should have received a copy of the GNU General Public License', 0)
	log('along with this program.  If not, see <http://www.gnu.org/licenses/>.', 0)
	log('', 0)
	print('SYNTAX: 1parse folderSRC folderimg [-v] [-i] [-d] [-fnn] [-p]')
	print('-v   Verbose mode')
	print('-f60   fps: take 1 picture each n seconds. Default fps=1/60 ie 1 picture per minute')
	print('-p   Parallel. Will not process if run flag is set')
	print('-c   Clean. Will not execute ffmpeg but will remove unfinished images: run.flag exist or incorrect fps.')
	print('-log=file   Log file')
	
	log ('foldervideo : ' + foldervideo, 1)
	log ('folderimg : ' + folderimg, 1)
	log ('fps : ' + fps, 1)
	log ('nb args : ' + str(len(sys.argv)-1), 3)
	log ('abspath' + os.path.abspath(foldervideo + '..'), 5)
	log ('basename' + os.path.basename(foldervideo), 5)
	log ('dirname' + os.path.dirname(foldervideo), 5)
	log ('', 5)
	log ('debug = ' + str(debug), 5)
	
	#Step 1: Delete obsolete images
	log ('************************************************************************************', 0)
	log (' Step 1: Delete obsolete images for ' + foldervideo, 0)
	log ('************************************************************************************', 0)
	BoucleSupp('')
	
	#Step 2: Create missing images		
	log ('************************************************************************************', 0)
	log (' Step 2: Create missing images for ' + foldervideo, 0)
	log ('************************************************************************************', 0)
	BoucleCount(foldervideo, folderimg, level)
	BoucleFichiers(foldervideo, folderimg, level)
	
log('************************************************************************************', 0)
log('* 1parse ' + foldervideo + ' ' + folderimg + txtgreen + ' DONE: ' + str(cptdone) + ' / ' + str(cpttodo) + txtnocolor, 0)
log('************************************************************************************', 0)
log('', 0)
flog.close

