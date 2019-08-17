#!/usr/bin/env python3

import sys
import os
import subprocess
#import fnmatch
#import shutil
#from os.path import join, getsize
#import hashlib
#import sqlite3
#import psycopg2
#from plog import plog
#import codecs
import time
#import datetime
from PIL import Image
import numpy
import gmpy2

#Declarations
txtgreen = '\033[0;32m'
txterr = '\033[0;33m'
txtnocolor = '\033[0m'
tmp = '/tmp/'

#log messages to log file and to screen
def log(s='', threshold=1):
    print(s)

def duration(d, dat=True):
    h = int(d // 3600)
    m = int((d - 3600*h) // 60)
    s = d - 3600*h - 60*m
    if d > 3600: r = '{:02d}'.format(h) + ' h ' + '{:02d}'.format(m)
    elif d > 60: r = '{:02d}'.format(m) + ' mn ' + '{:02.0f}'.format(s)
    else: r = '{:02.3f}'.format(s) + ' s'
    if dat:
        r = txtgreen + time.asctime(time.localtime(time.time())) + txtnocolor + ' ' + r
    return r

#Give the name of a file by removing the forder reference
def ShortName(fullname):
    k = len(fullname) - 1
    while (fullname[k] != '/') and (k > 0): 
      k = k - 1
    if k == 0:
      r = fullname
    else:
      r = fullname[k+1:]
    return r

def MidName(line, source=False):
    pend = len(line) - 1
    while (pend > 0) and (line[pend] != '/'): 
      pend = pend - 1
    if pend == 0:
      r = line
    else:
      pbeg = pend - 1
      while (pbeg >= 0) and (line[pbeg] != '/'):
        pbeg = pbeg - 1
      if source:
        r = line[pbeg+1:pend]
      else:
        r = line[pbeg+1:]
    return r 

#Give the name of a file by removing the forder reference
def PathName(fullname):
    k = len(fullname) - 1
    while fullname[k] != '/': k = k - 1
    return str(fullname[:k])

#Replace / in path to create an image file with reference to source path
def SlashToSpace(fullname, start=0):
    s = ''
    for k in range(start, len(fullname)):
        if fullname[k] == '/':
            s = s + ' '
        else:
            s = s + fullname[k]
    return s

def sortoccurence(elem):
    return elem[0]

def sortsources(elem):
    return elem[1][0] + elem[1][1]

def sortimages(elem):
    return elem[2][0] + elem[2][1]

def calcfp(file, quality, display=False):
  result = -1
  if os.path.exists(file):
    tmpfile = tmp + ShortName(file)
    log('File exists. tmpfile = ' + tmpfile)
    if os.path.splitext(file)[1] == '.jpg':
      if quality == 1:
          s = 'convert "' + file + '"[160x160] -modulate 100,0 -blur 3x99 -normalize -equalize -resize 16x16 -threshold 50% "' + tmpfile +'"'
      if quality == 2:
          s = 'convert "' + file + '"[160x160] -modulate 100,0 -blur 3x99 -normalize -equalize -resize 28x16 -threshold 50% "' + tmpfile +'"'
      if quality == 3:
          s = 'convert "' + file + '"[160x160] -modulate 100,0 -blur 3x99 -normalize -equalize -resize 57x32 -threshold 50% "' + tmpfile +'"'
      log('Fingerprint : ' + s, 0)
      p=subprocess.Popen(s, stdout=subprocess.PIPE, shell=True)
      (output, err) = p.communicate()
      if err == None:
        im = Image.open(tmpfile)
        img = numpy.asarray(im)
        key = '0b'
        for i in range(img.shape[0]):
            for j in range(img.shape[1]):
                if (img[i,j] < 128):
                    key = key + '0'
                else:
                    key = key +'1'
        result = int(key,2)
        if display:
            print('quality = ' + str(quality))
            print(result)
            for i in range(img.shape[0]):
                s = ''
                for j in range(img.shape[1]):
                    if (img[i,j] < 128):
                        s = s + '.'
                    else:
                        s = s +'X'
                print(s)
  else:
    log('File not exists : ' + file)
  return result

def loadunwanted(folder):
    global unwanted
    global uwpair

    if not(os.path.isdir(folder)):
      os.mkdir(folder, mode=0o777)

    uwpair = []
    #log(duration(time.perf_counter() - perf) + ' - Loading list of images to exclude from search from ' + folder, 1)
    for file in os.listdir(folder):
        if (os.path.splitext(file)[1] == '.jpg'):
            if (os.path.exists(folder + 'unwantedimages.fp')) and (os.path.getmtime(folder + 'unwantedimages.fp') < os.path.getatime(folder + file)):
                log('Rebuilt unwantedimages.fp due to recent files added.', 2)
                log(file + ' : ', 2)
                log(str(os.path.getatime    (folder + file)) + ' - ' + str(time.localtime(os.path.getatime(folder + file))), 2)
                log(str(os.path.getmtime    (folder + file)) + ' - ' + str(time.localtime(os.path.getmtime(folder + file))), 2)
                log(str(os.path.getctime    (folder + file)) + ' - ' + str(time.localtime(os.path.getctime(folder + file))), 2)
                log('unwantedimages.fp : ' + str(os.path.getmtime(folder + 'unwantedimages.fp')) + ' - ' + str(time.localtime(os.path.getmtime(folder + \
                  'unwantedimages.fp'))), 2)
                log('---', 2)
                os.remove(folder + 'unwantedimages.fp')
        if (os.path.splitext(file)[1] == '.txt'):
            f = open(folder + file, 'r')
            tmplist = []
            for line in f:
                if line[:5] == 'pair=':
                    tmplist.append(line[5:-1])
            f.close
            if tmplist in uwpair:
              print('Duplicate pair :')
              print(tmplist)
              os.remove(folder + file)
            else:
              uwpair.append(tmplist)

    if os.path.exists(folder + 'unwantedimages.fp'):
        f = open(folder + 'unwantedimages.fp', 'r')
        for key in f:
            unwanted.append(int(key[:-1]))
        f.close

    else:
        #log(duration(time.perf_counter() - perf) + ' - Cache unwantedimages.fp to rebuild. Around 5 files per second.')
        for file in os.listdir(folder):
            if (os.path.splitext(file)[1] == '.jpg'):
                key = calcfp(folder + file,1)
                if key in unwanted:
                    log(str(key) + ' : Other image with same key. Remove file from unwanted.', 1)
                    log(str(key) + ' : Other image with same key. Remove ' + folder + file, 4)
                    os.remove(folder + file)
                else:
                    unwanted.append(key)
                    log(str(key) + ' added in unwanted.', 2)

        f = open(folder + 'unwantedimages.fp', 'w')
        for key in unwanted:
            f.write(str(key) + '\n')
        f.close

    #log(duration(time.perf_counter() - perf) + ' - ' + str(len(unwanted)) + ' unwanted images fingerprinted and ' + str(len(uwpair)) + ' unwanted pairs of sources.')
                    
def helpprt():
    log('This program is free software: you can redistribute it and/or modify', copyright)
    log('it under the terms of the GNU General Public License as published by', copyright)
    log('the Free Software Foundation, either version 3 of the License, or', copyright)
    log('(at your option) any later version.', copyright)
    log('', copyright)
    log('This program is distributed in the hope that it will be useful,', copyright)
    log('but WITHOUT ANY WARRANTY; without even the implied warranty of', copyright)
    log('MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the', copyright)
    log('GNU General Public License for more details.', copyright)
    log('', copyright)
    log('You should have received a copy of the GNU General Public License', copyright)
    log('along with this program.  If not, see <http://www.gnu.org/licenses/>.', copyright)
    log('', copyright)
    log('SYNTAX : 3_debug_compare image1 image2', copyright)
    log('Usage: you can use 3_analyse with -v=2 or 3 to understand curious cases, then 3_debug_compare to dig in them.', copyright)
    log('', 0)

    
#main
log('************************************************************************************')
#read arguments and conform them
log('Video DeDup : find video duplicates')
log('Copyright (C) 2019  Pierre Crette')
log('')

print(sys.argv)

if len(sys.argv)<2:
    log('SYNTAX ERROR:')
    helpprt
    exit()

else:
    imagefile1 = os.path.normpath(sys.argv[1])
    imagefile2 = os.path.normpath(sys.argv[2])

    helpprt
    
    hdk1 = calcfp(imagefile1, 3, True)
    hdk2 = calcfp(imagefile2, 3, True)
    
    dist = gmpy2.hamdist(int(hdk1),int(hdk2))
    
    log('************************************************************************************')
    print(imagefile1)
    print(str(hdk1))
    print(imagefile2)
    print(str(hdk2))
    print('hamdist = ' + str(dist))
       
    if (hdk1 != hdk2):
      print('Key differents. HD distance = ' + str(dist))
    else:
      print('Identic HD keys. HD distance = 0')
    log('************************************************************************************')






































