#!/usr/bin/env python3

import sys
import os
#import fnmatch
#import shutil
#from os.path import join, getsize
#import hashlib
#import sqlite3
#import psycopg2
#from pprint import pprint
import time
import random
#from PIL import Image
#from PIL import ImageEnhance
#from PIL import ImageFilter
#import Pillow
#import numpy
#import subprocess
#from multiprocessing import Pool
#from multiprocessing.dummy import Pool as ThreadPool
import multiprocessing.dummy
import multiprocessing
import gmpy2

#Declarations
debug = 1
copyright = 0
parallel = 0
fp = []
path = []
comp = []
threshold = 10
threads = 3
mask = 1
masksize = 5
maskmethod = 'cycle'
#maskmethod = 'random'
checkall = False

pid = str(time.time())
script = 'resultset' + pid + '.txt'
logfile = 'log/2compare.' + pid + '.log'
foldervideo= '.'
folderimg = '.'
txtgreen = '\033[0;32m'
txtred = '\033[0;31m'
txtnocolor = '\033[0m'

nn = 0            
nbsrc = 0

#log messages to log file and to screen
def log(s='', threshold=1):
    flog.write(s + '\n')
    if debug >= threshold: print(s)

def duration(d, dat=True):
    h = int(d // 3600)
    m = int((d - 3600*h) // 60)
    s = d - 3600*h - 60*m
    if (d > 3600): r = '{:02d}'.format(h) + ' h ' + '{:02d}'.format(m)
    elif (d > 60): r = '{:02d}'.format(m) + ' mn ' + '{:02.0f}'.format(s)
    else: r = '{:02.3f}'.format(s) + ' s'
    if dat:
        r = txtgreen + time.asctime(time.localtime(time.time())) + txtnocolor + ' ' + r
    return r
  
def sortoccurence(elem):
    return elem[1]

#Give the name of a file by removing the forder reference
def ShortName(fullname):
    k = len(fullname) - 1
    while fullname[k] != '/': k = k - 1
    return fullname[k+1:]

#Give the name of a file by removing the forder reference
def PathName(fullname):
    k = len(fullname) - 1
    while fullname[k] != '/': k = k - 1
    return fullname[:k]

# Load fingerprints in memory
def BoucleFichiers(folderi='.'):
    global nn
    global nbsrc
    global fp
    fp1 = []

    log('BoucleFichiers ' + folderi, 4)
    perf = time.perf_counter()
    if os.path.isdir(folderi):
        if folderi[-1] != "/": folderi = folderi + "/"
        for file in os.listdir(folderi):
            ext = os.path.splitext(file)[1]
            if os.path.isdir(folderi + file):
                log(folderi + file + ' is a folder.', 4)
                BoucleFichiers(folderi + file)
            elif (ext.upper() == '.FP'):
                num_lines = sum(1 for line in open(folderi + file))
                maskloc = mask
                maskmethodloc = maskmethod
                if num_lines < 10*masksize*mask:
                  maskmethodloc = 'cycle'
                if num_lines < 6*masksize*mask:
                  maskloc = int(mask * 0.7)
                if num_lines < 3*masksize*mask:
                  maskloc = int(mask * 0.5)
                if num_lines < 1.5*masksize*mask:
                  maskloc = 1
                f = open(folderi + file, 'r')
                n = 0
                maskon = False
                for line in f:
                    line = line[:-1]
                    if line[:4] == 'key=':
                        key = int(line[4:])
                    if line[:5] == 'file=':
                        # Reshape after folder move
                        line = 'file=' + folderi + ShortName(line)
                        n = n + 1
                        file = line[5:]
                        if (maskloc < 2):
                            maskon = True
                        else:
                            if (n % masksize == 0):
                                if (maskmethodloc == 'cycle'):
                                    maskon = (((n // masksize) % maskloc) == 1)
                                else:
                                    maskon = (random.random() < 1/maskloc)
#                        log('bclfch: ' + str(n) + ' ' + str(maskon) + ', ' + str(nn) + ', ' + file, 4)
                        if maskon:
                            nn = nn + 1
                            fp1.append([key, line,  gmpy2.hamdist(key,0)])
                f.close
                if len(fp1)>0:
                  nbsrc = nbsrc + 1
                  pn = PathName(fp1[0][1])
                  fp.append([pn, ShortName(folderi[:-1]), fp1])
                  path.append([pn, 1])
                else:
                  log('Not enough .jpg or mask error : ' + folderi)
                  log('mask = ' + str(mask) + ', masksize = ' + str(masksize) + ', maskmethod = ' + maskmethod, 2)
                #log(str(len(fp)) + ' loaded fingerprints in ' + str(time.perf_counter() - perf) + ' sec.', 0)
    else:
        log('folderi = ' + folderi, 0)
        #OneFile(os.path.dirname(folderi)+"/",os.path.basename(folderi))
    
    if folderi == folderimg:
        log('{:_}'.format(len(fp)) + ' sources, {:_}'.format(nbsrc) + ' left sources and {:_}'.format(nn) + ' fingerprints loaded in ' + duration(time.perf_counter() - perf, False), 1)
        fp = sorted(fp, key=sortoccurence)
    else:
        log('{:_}'.format(len(fp)) + ' sources, {:_}'.format(nbsrc) + ' left sources and {:_}'.format(nn) + ' fingerprints loaded in ' + duration(time.perf_counter() - perf, False), 3)

#fp is a    list of:
#    folderimg of a source
#    short source file name
#    list of:
#        key: integer
#        image file name
#        bitcount of key

#CompareOne is the thread
def CompareOneNext(compelt):
    global fp
    global path
    
    localdouble = []
    n = compelt[0]           # left position
    cur = compelt[1]         # image of n
    for i in range(n+1, len(fp)):                               # i = other sources
        for j in range(len(fp[i][2])):                          # j = images for source i
#            if (abs(fp[n][1][cur][2] - fp[i][1][j][2]) <= threshold):
                r = gmpy2.hamdist(fp[n][2][cur][0], fp[i][2][j][0])
                if r < threshold:
                    localdouble.append([fp[n][2][cur][0],fp[n][2][cur][1],fp[i][2][j][0],fp[i][2][j][1],int(r)])

    return localdouble

def CompareOneAll(compelt):
    global fp
    global path
    
    localdouble = []
    n = compelt[0]           # left position
    cur = compelt[1]         # image of n
    for i in range(len(fp)):                               # i = other sources
      if i != n:
        for j in range(len(fp[i][2])):                          # j = images for source i
#            if (abs(fp[n][1][cur][2] - fp[i][1][j][2]) <= threshold):
                r = gmpy2.hamdist(fp[n][2][cur][0], fp[i][2][j][0])
                if r < threshold:
                    localdouble.append([fp[n][2][cur][0],fp[n][2][cur][1],fp[i][2][j][0],fp[i][2][j][1],int(r)])

    return localdouble

def lock(n, lockfile):
    global fp
    global param
    
    todo = True
    if os.path.exists(lockfile + '.run') or os.path.exists(lockfile + '.done'):
      todo = False
    else:
      try:
        f = open(lockfile + '.run', 'w')
        f.write(pid + '\n')
        f.close
      except:
        todo = False
      time.sleep(3)
      if not(os.path.exists(lockfile + '.run')):
          todo = False
      if todo:
          with open(lockfile + '.run') as f:  
              line = f.readline()
              line = line[:-1]
          if line != pid:
              todo = False
    return todo

# Compare fingerprints in memory
def Compare():
    global fp
    global path
    global nn
    global nbsrc
    global perf
    
    log('Begin comparison of {:_}'.format(len(fp)) + ' fingerprints.', 0)

    nbcompared = 0
    for n in range(len(fp)):
      nbcompared = nbcompared + 1
      lockfile = fp[n][0][5:] + '/compare.' + param 

      if lock(n, lockfile):
        comp = []
        double = []
        for cur in range(len(fp[n][2])):                # cur = images for source n
          comp.append([n, cur])
    
        pool = multiprocessing.Pool(threads)
        
        if checkall:
          results = pool.map(CompareOneAll, comp)
        else:
          results = pool.map(CompareOneNext, comp)
          
        for i in range(len(results)):
#           log('i=' + str(i) + ' results[i]=' + results[i], 4)
            if len(results[i]):
                #log('Not empty :',4)
                for j in range(len(results[i])):
                    if debug > 1:
                        print('Add :')
                        print(results[i][j])
                    double.append(results[i][j])
        log('New doubles for ' + fp[n][0] + ' = {:_}'.format(len(double)), 2)
        
        #close the pool and wait for the work to finish
        pool.close()
        pool.join()
        
        f = open(script, 'a')
        for i in range(0,len(double)):
            f.write('BEGIN. Similarity=' + str(double[i][4]) + '\n')
            f.write(double[i][1] + '\n')
            f.write('key=' + str(double[i][0]) + '\n')
            f.write(double[i][3] + '\n')
            f.write('key=' + str(double[i][2]) + '\n')
            f.write('END' + '\n')
            if PathName(double[i][1]) == PathName(double[i][3]):
              log('ERROR: Theorically no still images should be detected because left and right source are supposed to be different.', 0)
        f.close
        
        os.rename(lockfile + '.run', lockfile + '.done')
    
        log(duration(time.perf_counter() - perf) + ' - {:_}'.format(nbcompared) + ' sources compared so far on {:_}'.format(nbsrc) , 0)
      
      else:
        log(duration(time.perf_counter() - perf) + ' - Skip #{:_}'.format(nbcompared), 1)
        log('Skip #' + lockfile, 2)

def prthelp():
    log('************************************************************************************', 0)
    log('Video DeDup - module compare : find video duplicates', 0)
    log('Copyright (C) 2018  Pierre Crette', 0)
    log('', 0)
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
    log('SYNTAX: 2compare folderimg [options]', copyright)
    log('-v=n         Verbose mode. Default 1', copyright)
    log('-s=file      Script to log result founds.', copyright)
    log('-lbl=label   Label to identify runs with different parameters. Use the same on all sessions/computers to share workload. No special characters since its use for file naming.', copyright)
    log('-t=n         Threshold for similarity comparison. Default 10. Performance impact.', copyright)
    log('-threads=n   Number of threads to use. Make tests to find better option for your computer. Performance impact.', copyright)
    log('-mask=n      To limit the comparison to some images files for each source file. 1/n images are used. Performance impact.', copyright)
    log('-masksize=n  Read n images per source then skip (mask-1)*n images', copyright)
    log('-maskmethod= cycle: read n images per source then skip (mask-1)*masksize images, random: if random read masksize images else skip maxsize images.', copyright)
    log('-checkall    Compare new sources/images against ALL other. By default only against NEXT.', copyright)
    log('-log=file  Log file', copyright)
    log('', copyright)
    log('If last line of script is not FINISHED then the program was interupted but the partial result is exploitable.', copyright)
    log('', copyright)
  
#main

#Step0: Read arguments and initialize variables
print ('')
#print (str(sys.argv))
flog = open(logfile,'w')
if len(sys.argv)<1:
    prthelp
    sys.exit()
else:
    if not(os.path.exists('log')):
        os.makedirs('log')

    folderimg = os.path.normpath(sys.argv[1])
    if folderimg[-1] != "/": folderimg = folderimg + "/"
    if not(os.path.exists(folderimg)):
        os.makedirs(folderimg)

#    s = '2compare.py ' + folderimg
    lbl = ''
    for i in sys.argv[2:]:
#        s = s + ' ' + i
        if i[:3]  == '-v=':          debug = int(i[3:])
        if i[:5]  == '-log=':        logfile = i[5:]
        if i[:3]  == '-s=':          script = i[3:]
        if i[:5]  == '-lbl=':        lbl = i[5:]
        if i[:3]  == '-t=':
            threshold = int(i[3:])
            log(txtred + 'WARNING :' + txtnocolor)
            log('It is wise to let a large limit because you can narrow the results with 3analyse -maxdiff option but this parameter is also a performance hit.')
        if i[:3]  == '-nc': copyright = 12
        if i[:9]  == '-threads=':    threads = int(i[9:])
        if i[:6]  == '-mask=':       mask = int(i[6:])
        if i[:10] == '-masksize=':   masksize = int(i[10:])
        if i[:12] == '-maskmethod=': maskmethod = i[12:]
        if i[:9] == '-checkall' :    checkall = True

    param = lbl + '_t_' + str(threshold) + '_' + maskmethod + '_' + str(mask) + '_' + str(masksize)
    log('param = ' + param)

    prthelp
    log ('folderimg : ' + folderimg, 5)
    log ('nb args : ' + str(len(sys.argv)-1), 5)
    log ('', 5)
    log ('debug = ' + str(debug), 5)
    log ('', 0)
    log ('************************************************************************************', 0)
    log (' Find duplicate images in ' + folderimg, 0)
    log ('************************************************************************************', 0)
    
    log(duration(0) + ' - Locate *.fp and load into memory...',1)
    BoucleFichiers(folderimg)
    
    Compare()

    f = open(script, 'a')
    f.write('FINISHED' + '\n')
    f.close

log('************************************************************************************', 0)
log('* Compare ' + folderimg + txtgreen + ' DONE' + txtnocolor, 0)
log('************************************************************************************', 0)
log('', 0)
flog.close

