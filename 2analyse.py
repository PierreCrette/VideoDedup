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
import codecs

#Declarations
debug = 1
env = 'prd'

#Give the name of a file by removing the forder reference
def ShortName(fullname):
	k = len(fullname) - 1
	while fullname[k] != '/': k = k - 1
	return fullname[k+1:]

#Replace / in path to create an image file with reference to source path
def SlashToSpace(fullname, start):
	s = ''
	for k in range(start, len(fullname)):
		if fullname[k] == '/':
			s = s + ' '
		else:
			s = s + fullname[k]
	return s

#main
print('************************************************************************************')
#read arguments and conform them
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
	
if debug>0: print(sys.argv)
if len(sys.argv)<3:
	print('SYNTAX ERROR: 2analyse foldersrc folderimg findimagedupesresult [n]')
	halt
else:
	foldervideo = os.path.normpath(sys.argv[1])
	if foldervideo[-1] != "/": foldervideo = foldervideo + "/"
	folderimgraw = os.path.normpath(sys.argv[2])
	if folderimgraw[-1] != "/":
		folderimg = folderimgraw + "/db/"
		folderana = folderimgraw + "/ana-" + env + "-not-saved/"
	else:
		folderimg = folderimgraw + "db/"
		folderana = folderimgraw + "ana-" + env + "-not-saved/"
	fresultset = os.path.normpath(sys.argv[3])
	if debug>2:
		print('folderimg = ' + folderimg)
		print('folderana = ' + folderana)
		print('fresultset = ' + fresultset)
	if not(os.path.exists(folderana)): os.mkdir(folderana, mode=0o777)
	threshold = 1
	if len(sys.argv) > 3: threshold = int(sys.argv[4])
	if debug>0: print ('Consider double if at least ' + str(threshold) + ' images are similar in the set.')
	
	#Step 1: parse fresultset and create memory map
	resultsetvideo = []
	setvideo = []
	setimg = []
	setprt = []
	
	f = open(fresultset, 'r')
	for line in f:
		line = line[:-1]
		
		if line == 'BEGIN':
			#Initiate a new set of doubles files
			setvideo = []
			setimg = []
			setprt = []
			
		if line == 'END':
			#Close the set
			#If the set contains at least 2 source video files
			if len(setvideo) > 1:
				print('*******************************************')
				#Seek if the set is already known
				setvideo.sort
				new = -1
				for j in range(len(resultsetvideo)):
					if debug>1: print('1: ')
					if debug>1: print(resultsetvideo[j][1])
					if debug>1: print('')
					if resultsetvideo[j][1] == setvideo: 
						print('***** Resultset existant trouve *****')
						new = j
				if new < 0:
					#New set of duplicates
					resultsetvideo.append([1, setvideo, setimg, setprt])
					if debug>0: print(setvideo)
				else:
					#Update existing set
					x = resultsetvideo[new]
					resultsetvideo[new] = [x[0] + 1, x[1], x[2] + setimg, x[3] + setprt]
					if debug>0: print('Avant update de set :')
					if debug>0: print(x)
					if debug>0: print('Apres :')
					if debug>0: print(resultsetvideo[new])
						
		if len(line) > 5:
			#Action on 1 image file of a set of doubles
			setimg.append(line)
			s = '/'
			for j in range(len(folderimg),len(line)):
				if line[j] == '/':
					s = s + ' '
				else:
					s = s + line[j]
			setprt.append(s)
			#Identify the source video file of this image
			k = len(line) - 1
			while line[k] != "/": k = k - 1
			src = foldervideo + line[len(folderimg):k]
			#This source video file is already in set ? Case of a still or repeted image in the movie
			new = True
			for d in setvideo:
				if d == src: new = False
			if new:
				setvideo.append(src)

	#Step 2: create Analyse folder and copy all files in it
	for j in range(len(resultsetvideo)):
		fld = folderana + str(j) + '/'
		x = resultsetvideo[j]
		if x[0] >= threshold:
			if not(os.path.exists(fld)):
				os.mkdir(fld, mode=0o777)
			else:
				shutil.rmtree(fld)
				os.mkdir(fld, mode=0o777)
				
			#x[1] are Video source files
			for d in enumerate(x[1]):
				if debug>0:
					print('Copy ' + d[1] + ' ' + fld + SlashToSpace(d[1], len(foldervideo)))
				if os.path.exists(d[1]):
					shutil.copy2(d[1], fld + SlashToSpace(d[1], len(foldervideo)))
				else:
					if debug>0: print('Not exist ' + d[1])

			#x[2] are images files
			f = open(fld + '/nb_match_' + str(x[0]) + '.txt','w')
			for d in enumerate(x[2]):
				f.write(d[1] + '\n')
				if debug>0: 
					print(fld + SlashToSpace(d[1], len(folderimg)))
				if os.path.exists(d[1]): 
					shutil.copy2(d[1],fld + SlashToSpace(d[1], len(folderimg)))
				else:
					if debug>0: print('Not exist ' + d[1])
			f.close

