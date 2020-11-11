#!/usr/bin/env python3

import sys
import os
import subprocess
import fnmatch
import shutil
import time
import random
from PIL import Image
import numpy
import multiprocessing

#Declarations
debug = 1
copyright = 0
cpttodo = 0
cptdone = 0
level = 0
fpsn=60.0
MinJpgCount=5
threads=3
parallel = False
clean = False
fast = False
moved = True
renamed = False
srclst = []
pid = str(time.time())
logfile = 'log/1parse.' + pid + '.log'
foldervideo= '.'
folderimg = '.'
mpdata = []
txtgreen = '\033[0;32m'
txtred = '\033[0;31m'
txtnocolor = '\033[0m'
jobcounter = 0

#log messages to log file and to screen
def log(s='', threshold=1):
    flog.write(s + '\n')
    if debug >= threshold: print(s)

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

def sortoccurence(elem):
    return elem[0]

def SameSrcNames():
  global srclst, renamed

  srclst = sorted(srclst, key=sortoccurence)
  log(duration(time.perf_counter() - perf) + ' - Test of source with same name. len(srclst)=' + str(len(srclst)), 0)
  log(duration(time.perf_counter() - perf) + ' - First one is ' + srclst[0][0], 2)
  prev = ['']
  firstrenamed = False
  for i in range(len(srclst)):
    #DEBUG : the prev file must be renamed also to avoid pointing on a bad image
    if srclst[i][0] == prev[0]:
      renamed = True
      log(duration(time.perf_counter() - perf) + ' - File ' + srclst[i][1] + srclst[i][0] + ' is referenced multiples times. Renaming all of them.', 1)
      log(duration(time.perf_counter() - perf) + ' - ... renaming to ' + srclst[i][1] + str(i) + srclst[i][0], 1)
      if not(firstrenamed):
        log(duration(time.perf_counter() - perf) + ' - File ' + prev[1] + prev[0] + ' is referenced multiples times. Renaming all of them.', 2)
        log(duration(time.perf_counter() - perf) + ' - ... renaming to ' + prev[1] + str(i-1) + prev[0], 1)
        os.rename(prev[1] + prev[0], prev[1] + str(i-1) + prev[0])
        firstrenamed = True
      os.rename(srclst[i][1] + srclst[i][0], srclst[i][1] + str(i) + srclst[i][0])
    else:
      firstrenamed = False
    prev = srclst[i]

def vidimgfolder(extention=''):
  return ((extention == '.MP4') or (extention == '.AVI') or (extention == '.MOV') or (extention == '.M4V') or (extention == '.VOB') \
       or (extention == '.MPG') or (extention == '.MPEG') or (extention == '.MKV') or (extention == '.WMV') or (extention == '.ASF') \
       or (extention == '.FLV') or (extention == '.RM') or (extention == '.OGM') or (extention == '.M2TS') or (extention == '.RMVB'))

#Step1: remove images with no more source
def BoucleSupp(radical='', root=True):
  #executed only in clean mode
  global srclst

  if radical != "":
    if radical[-1] != "/": radical = radical + "/"
  if root:
    log(duration(time.perf_counter() - perf) + ' - BoucleSupp(' + radical + ') will move images if source moved or delete it.', 1)

  ext = os.path.splitext(folderimg + radical)[1]
  ext = ext.upper()
  notvidimgfolder = not (vidimgfolder(ext))
  if os.path.isdir(folderimg + radical):
    fcount = 0
    for file in os.scandir(folderimg + radical):
      fcount = fcount + 1
      fname = file.name
      ext = os.path.splitext(fname)[1]
      ext = ext.upper()
      if file.is_dir():
        log('file = ' + fname + ', ext = ' + ext, 2)
        if vidimgfolder(ext):
          srcfilename = foldervideo + radical + fname
          if not(os.path.exists(srcfilename)):
            found = False
            for newsrc in srclst:
              if newsrc[0] == fname:
                found = True
                mvsrc = folderimg + radical + fname + '/'
                mvdst = newsrc[2] + fname + '/'
                log(duration(time.perf_counter() - perf) + ' - Moving ' + mvsrc + ' to ' + mvdst, 0)
                try:
                  print(newsrc[2])
                  print(fname)
                  shutil.move(folderimg + radical + fname, newsrc[2])
                except:
                  log(duration(time.perf_counter() - perf) + ' - ' + txtred + 'Error' + txtnocolor + ' when moving image folder. Try to remove ' + folderimg + radical + fname, 0)
                if os.path.exists(folderimg + radical + fname):
                  log(duration(time.perf_counter() - perf) + ' - deleting ' + folderimg + radical + fname, 0)
                  try:
                    shutil.rmtree(folderimg + radical + fname)
                  except:
                    log(duration(time.perf_counter() - perf) + ' - ' + txtred + 'Error' + txtnocolor + ' when deleting image folder ' + folderimg + radical + fname, 0)
            if not(found):
              log(duration(time.perf_counter() - perf) + ' - ' + txtred + 'Error' + txtnocolor + ' not found ' + fname + '. The file was deleted. Removing the image folder.', 1)
              shutil.rmtree(folderimg + radical + fname)
          else:
            if not(fast):
              #log(duration(time.perf_counter() - perf) + ' - test ' + folderimg + radical + fname + '/*.jpg', 2)
              if not(os.path.exists(folderimg + radical + fname + '/img00001.jpg')):
                log(duration(time.perf_counter() - perf) + ' - Delete because no .jpg image : ' + folderimg + radical + fname, 0)
                shutil.rmtree(folderimg + radical + fname)
        else:
          BoucleSupp(radical + fname, False)
      else:
        if notvidimgfolder:
          # print(folderimg + radical + file.name + ' -> ' + os.path.splitext(file)[1])
          if (file.name == 'fingerprint.fp') or (file.name == 'param.txt') or (os.path.splitext(file)[1] == '.src') or (os.path.splitext(file)[1] == '.jpg'):
            log('Misplaced files. Deleting ' + folderimg + radical + file.name, 0)
            os.remove(folderimg + radical + file.name)
      
    if fcount == 0:  
      log(duration(time.perf_counter() - perf) + ' - Delete empty folder ' + folderimg + radical, 2)
      shutil.rmtree(folderimg + radical)
          
#        if not(fast):
#          sp = radical + fname
#          lp = len(sp) - 1 - 2 * len(fname)
#          if sp[lp:] == fname + '/' + fname:
#            log(duration(time.perf_counter() - perf) + ' - Delete because image was corrupted: ' + folderimg + radical + fname, 0)
#            shutil.rmtree(folderimg + radical)        

#Count source to do
def BoucleCount(folderv='.', folderi='.', level=1):
  global cpttodo
  global srclst

  level = level + 1
  spacer = ''
  if debug>1:
    for i in range(level): spacer=spacer+'  '
    log(spacer + '[ ' + folderv, 1)
  if os.path.isdir(folderv):
    if not(os.path.exists(folderi)):
      os.mkdir(folderi, mode=0o777)
    if folderv[-1] != "/": folderv = folderv + "/"
    if folderi[-1] != "/": folderi = folderi + "/"
    for file in os.scandir(folderv):
      fname = file.name
      ext = os.path.splitext(fname)[1]
      ext = ext.upper()
      if os.path.isdir(folderv + fname):
        BoucleCount(folderv + fname, folderi + fname, level+1)
      elif (ext == '.MP4') or (ext == '.AVI') or (ext == '.MOV') or (ext == '.M4V') or (ext == '.VOB') or (ext == '.MPG') or (ext == '.MPEG') \
      or (ext == '.MKV') or (ext == '.WMV') or (ext == '.ASF') or (ext == '.FLV') or (ext == '.RM') or (ext == '.OGM') or (ext == '.M2TS') \
      or (ext == '.RMVB'):
        nameerror = False
        for c in fname:
          if (c in [';',' ']):
            nameerror = True
        if nameerror:
          log('ERROR in naming convention for ' + fname, 0)
          sys.exit(1)
        cpttodo = cpttodo + 1
        srclst.append([fname, folderv, folderi])
      elif not(ext == '.JPG' or ext == '.TXT' or ext == '.TXT~' or ext == '.SRC'):
        log(spacer + '  Not match : ' + folderv + fname, 2)

  if debug>1:
      spacer = ''
      for i in range(level): spacer=spacer+'  '
      log(spacer + '  ' + folderv + ' count = ' + str(cpttodo) + ' ]', 1)
  level = level - 1

def mpimagemagick(elt, queue):
  global fpsn, perf, tmp

  def mpcalcfp(folder, file):
    result = []
    if os.path.splitext(file)[1] == '.jpg':
      s = 'convert "' + folder + '/' + file + '"[160x160] -modulate 100,0 -blur 3x99 -normalize -equalize -resize 16x16 -threshold 50% "' + tmpmp + file +'"'
      p=subprocess.Popen(s, stdout=subprocess.PIPE, shell=True)
      (output, err) = p.communicate()
      if err == None:
        if os.path.exists(tmpmp + file):
          im = Image.open(tmpmp + file)
          img = numpy.asarray(im)
          key = '0b'
          for i in range(img.shape[0]):
            for j in range(img.shape[1]):
              if (img[i,j] < 128):
                key = key + '0'
              else:
                key = key +'1'
          result = [int(key,2),folder+file]
    return result

  def mpCreateFingerprint(folder=''):
    todo = True
    if folder[-1] != "/": folder = folder + "/"
    if not(os.path.isdir(folder)):
      log(duration(time.perf_counter() - perf) + ' - CreateFingerprint cannot run before ffmpeg for ' + folder, 0)
      todo = False
    if len(fnmatch.filter(os.listdir(folder), '*.jpg')) < MinJpgCount:
      log(duration(time.perf_counter() - perf) + ' - ' + txtred + 'ERROR: '  + txtnocolor + folder + ' contains not enough JPG. Removing it and move source video in Error subfolder.', 0)
      shutil.rmtree(folder)
      os.remove(mpfolderi + mpfile + '.run')
      srcf = mpfvideo[:len(mpfvideo) - len(mpfile)]
      if srcf[-6:] != 'error/':
        #print('srcf[-6:] = ' + srcf[-6:])
        if not os.path.exists(srcf + 'error/'):
          os.makedirs(srcf + 'error/')
        shutil.move(mpfvideo, srcf + 'error/' + mpfile)
      todo = False

    if todo:
      todo = not(os.path.exists(folder + 'fingerprint.fp'))

    if todo:
      log(duration(time.perf_counter() - perf) + ' - CreateFingerprint start for folder ' + folder, 1)
      fpram = []
      for file in os.listdir(folder):
        if os.path.splitext(file)[1].upper() == ".JPG":
          k = mpcalcfp(folder,file)
          if k == []:
            todo = False
          else:
            fpram.append(k)

      if todo:
        if not(os.path.exists(folder + 'fingerprint.fp')):
          fp = open(folder + 'fingerprint.fp','w')
          for i in range(len(fpram)):
            fp.write('key=' + str(fpram[i][0]) + '\n')
            fp.write('file=' + fpram[i][1] + '\n')
          fp.close
          log(duration(time.perf_counter() - perf) + ' - CreateFingerprint done for ' + folder, 1)

  (mpfolderi, mpfvideo, mpfile, mpargs, mps, cptdone, cpttodo) = elt
  mpperf = time.time()
  tmpmp = tmp[:-1] + '.' + str(cptdone) + '/'
  os.makedirs(tmpmp)
  log(duration(time.perf_counter() - perf) + ' - MP {}/{} '.format(cptdone,cpttodo) + 'Call ffmpeg with folderi = ' + mpfolderi + ' file = ' + mpfile, 2)
  #if not(os.path.exists(mpfolderi)):
    #os.mkdir(mpfolderi, mode=0o777)
  if not(os.path.exists(mpfolderi + mpfile + '/')):
    log('ERROR folder not exists ' + mpfolderi + mpfile, 0)
    #os.mkdir(mpfolderi + mpfile + '/', mode=0o777)
  else:
    with open(mpfolderi + mpfile + '.run') as f:
      line = f.readline()
      line = line[:-1]
    if line != pid:
      log(duration(time.perf_counter() - perf) + '    MP ---------------------------------------------------------------------------------------------------', 0)
      log(duration(time.perf_counter() - perf) + '    MP --- Concurent ffmpeg run detected !', 0)
      log(duration(time.perf_counter() - perf) + '    MP --- .run flag for ' + mpfvideo + mpfile + ' Skip due to parallel mode ', 0)
      log(duration(time.perf_counter() - perf) + '    MP ---------------------------------------------------------------------------------------------------', 0)
    else:
      #Call ffmpeg
      log(duration(time.perf_counter() - perf) + ' - MP {}/{} '.format(cptdone,cpttodo) + txtgreen + mps + txtnocolor, 1)
      p=subprocess.Popen(mpargs, stdout=subprocess.PIPE, close_fds=True)
      p.wait()
      (output, err) = p.communicate()
      #p.stdout.close()
      siz = os.path.getsize(mpfvideo)/1048576
      dur = time.time() - mpperf
      log(duration(time.perf_counter() - perf) + ' - MP {}/{} '.format(cptdone,cpttodo) + 'Duration ffmpeg: ' + duration(dur, False) + \
        ' for ' + str(round(siz,0)) + ' Mb ' + txtgreen + '@ ' + str(round(threads * siz / dur * 0.0864,2)) + ' Tb/day' + txtnocolor, 1)
  
      mpCreateFingerprint(mpfolderi + mpfile)
  
      #Create a file to store parameters
      if os.path.exists(mpfolderi + mpfile + '/fingerprint.fp'):
        if os.path.exists(mpfolderi + mpfile):
          f = open(mpfolderi + mpfile + '/param.txt','w')
          f.write('fps=1/' + str(fpsn) + '\n')
          f.close
        else:
          log('Cannot create param.txt bacause folder doesnt exists ' + mpfolderi + mpfile, 0)
        dur = time.time() - mpperf
        log(duration(time.perf_counter() - perf) + ' - MP {}/{} '.format(cptdone,cpttodo) + 'Duration ffmpeg + fingerprint: ' + duration(dur, False) + \
          ' for ' + str(round(siz,0)) + ' Mb ' + txtgreen + '@ ' + str(round(threads * siz / dur * 0.0864,2)) + ' Tb/day' + txtnocolor, 0)
        os.remove(mpfolderi + mpfile + '.run')
      else:
        log(mpfolderi + mpfile + '/fingerprint.fp doesnt exists.', 0)
    shutil.rmtree(tmpmp)

#Generate jpg images files for one source video file
def OneFile(folderv, folderi, file):
  global cpttodo, cptdone, mpdata, jobcounter

  def CreateSrc(folder):
    if not(os.path.exists(folder + '/' + file + '.src')):
      fp = open(folder + '/' + file + '.src','w')
      fp.write(file + '\n')
      fp.close
        
  #Initialization
  fvideo = folderv + file
  fimg = folderi + file + '/img%05d.jpg'
  log('OneFile(' + folderv +', ' + folderi + ', ' + file + ')', 2)

  #debug: works if shell=true. But security issue with shell=true in popen.
  if debug>1:
    s = 'ffmpeg -i "' + fvideo + '" -vf fps=1/' + str(fpsn * (0.9 + random.random()/4)) + ' "' + fimg + '"'
  else:
    s = 'ffmpeg -loglevel fatal -i "' + fvideo + '" -vf fps=1/' + str(fpsn * (0.9 + random.random()/4)) + ' "' + fimg + '"'

  #debug: if shell=false
  args=[]
  args.append('ffmpeg')
  if debug < 2:
    args.append('-loglevel')
    args.append('fatal')
  args.append('-i')
  args.append(fvideo)
  args.append('-vf')
  args.append('fps=1/' + str(fpsn * (0.9 + random.random()/4)))
  args.append(fimg)

  folderi2 = folderi + file

  # Controls
  todo = True
  if os.path.exists(folderi2):
    line = 'fps=1/60'
    if os.path.exists(folderi2 + '/param.txt'):
      with open(folderi2 + '/param.txt') as f:
        line = f.readline()
        line = line[:-1]
    if len(line) <= 6:
      log(duration(time.perf_counter() - perf) + '     --- Param.txt inconsistent : ' + line, 1)
      line = 'fps=1/999'
    if float(line[6:]) <= fpsn:
      if len(fnmatch.filter(os.listdir(folderi2), '*.jpg')) < MinJpgCount:
        log(duration(time.perf_counter() - perf) + ' - ' + txtred + 'ERROR: ' + folderi2 + ' contains not enough JPG. Removing it.' + txtnocolor, 0)
        shutil.rmtree(folderi2)
      else:
        todo = False
        CreateSrc(folderi2)
        log('   --- ffmpeg done ' + folderi2, 2)
    else:
      if parallel:
        log(duration(time.perf_counter() - perf) + ' - ' + folderi2 + ' ffmpeg done but upgrade from ' + line + ' to fps=1/' + str(fpsn), 1)
  else:
    if clean:
      log(duration(time.perf_counter() - perf) + ' - ' + folderi2 + ' does not exist. To do.', 2)
    else:
      log(duration(time.perf_counter() - perf) + ' - ' + folderi2 + ' does not exist. To do.', 1)

  #Cleanup based on startover mechanism
  if clean:
    if os.path.exists(folderi2 + '.run'):
      log(duration(time.perf_counter() - perf) + ' - ' + txtgreen + 'CLEAN image due to lock : ' + folderi2 + txtnocolor, 0)
      os.remove(folderi2 + '.run')
      if os.path.exists(folderi2):
        shutil.rmtree(folderi2)
    if os.path.exists(folderi2):
      if os.path.exists(folderi2 + '/fingerprint.run'):
        log(duration(time.perf_counter() - perf) + ' - ' + txtgreen + 'CLEAN due to fingerprint.run : ' + folderi2 + txtnocolor, 0)
        shutil.rmtree(folderi2)
      elif not(os.path.exists(folderi2 + '/fingerprint.fp')):
        log(duration(time.perf_counter() - perf) + ' - ' + txtgreen + 'CLEAN due to fingerprint empty : ' + folderi2 + txtnocolor, 0)
        shutil.rmtree(folderi2)
      elif todo:
        log(duration(time.perf_counter() - perf) + ' - ' + txtgreen + 'CLEAN due to parameters: ' + folderi2 + txtnocolor, 0)
        shutil.rmtree(folderi2)

  #Lock mechanism for startover procedure and parralel mode
  if (todo and parallel):
    if os.path.exists(folderi2 + '.run'):
      todo = False
      log(duration(time.perf_counter() - perf) + '     --- .run flag for ' + folderv + file + ' Skip due to parallel mode ', 0)
    else:
      log('set ' + folderi2 + '.run flag',2)
      f = open(folderi2 + '.run','w')
      f.write(pid + '\n')
      f.close

  # Execute
  if (todo and parallel):
    if not(os.path.exists(folderi2)):
      os.mkdir(folderi2 + '/', mode=0o777)
    CreateSrc(folderi2)
    #fire off workers

    # print([folderi, fvideo, file, args, s, cptdone, cpttodo])

    job = pool.apply_async(mpimagemagick, ([folderi, fvideo, file, args, s, cptdone, cpttodo], queue))
    jobs.append(job)    
    if not(queue.full):
      log('Execute 1 thread in the BoucleFichiers', 1)
      jobs[jobcounter].get()
      jobcounter = jobcounter + 1
    else:
      log('Queue full. Keep lock for later execution.', 1)

  cptdone = cptdone + 1
  if clean:
    log(duration(time.perf_counter() - perf) + ' - ' + txtgreen + str(cptdone) + ' / ' + str(cpttodo) + ' queued ...' + txtnocolor, 2)
  else:
    log(duration(time.perf_counter() - perf) + ' - ' + txtgreen + str(cptdone) + ' / ' + str(cpttodo) + ' queued ...' + txtnocolor, 1)

# Parse a single folder to call OneFile for source video files and BoucleFichier recursively if it'a a subfolder
def BoucleFichiers(folderv='.', folderi='.', level=1):
  global mpdata

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

    for file in os.scandir(folderv):
      fname = file.name
      ext = os.path.splitext(fname)[1]
      ext = ext.upper()        
      if file.is_dir():
        BoucleFichiers(folderv + fname, folderi + fname)
      elif (ext == '.MP4') or (ext == '.AVI') or (ext == '.MOV') or (ext == '.M4V') or (ext == '.VOB') or (ext == '.MPG') or (ext == '.MPEG') or (ext == '.MKV') \
        or (ext == '.WMV') or (ext == '.ASF') or (ext == '.FLV') or (ext == '.RM') or (ext == '.OGM') or (ext == '.M2TS') or (ext == '.RMVB'):
        OneFile(folderv, folderi, fname)
      elif not(ext == '.JPG' or ext == '.TXT' or ext == '.TXT~'):
        log(spacer + '  Not match : ' + folderv + fname, 0)
  else:
    log('folderv = ' + folderv, 0)
    OneFile(os.path.dirname(folderv)+"/",os.path.basename(folderv))
  if debug>1:
    spacer = ''
    for i in range(level): spacer=spacer+'  '
    log(spacer + folderv +  ' ]', 0)
  level = level - 1

def listener(queue):
  '''listens for messages on the queue, writes to file. '''
  while 1:
    m = queue.get()
    if m == 'kill':
      log('Normal Queue killed since all work done.', 1)
      break
            
def helpprt():
    log('************************************************************************************', 0)
    log('Video DeDup : find video duplicates', 0)
    log('Copyright (C) 2020  Pierre Crette', 0)
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
    log('SYNTAX: 1parse folderSRC folderimg [-v] [-i] [-d] [-fnn] [-p]', copyright)
    log('-v=n        Verbose mode', copyright)
    log('-f=n        fps: take 1 picture each n seconds. Default fps=1/60 ie 1 picture every 60 seconds.', copyright)
    log('-tmp=' + tmp + '  working temporary folder. /tmp/ by default. A Ram drive of 64Mb can improve performance.', copyright)
    log('-p          Parallel. Will not process if run flag is set', copyright)
    log('-c          Clean. Will not execute ffmpeg but will remove unfinished images: run.flag exist or incorrect fps or moved sources or no jpg.', copyright)
    log('-fastclean  Clean with less controls. Only .run and source movements are managed.', copyright)
    log('-threads=n  Number of threads to use.', copyright)
    log('-log=file   Log file', copyright)
    log('', copyright)


#main

#Step0: Read arguments and initialize variables
print ('')
#print (str(sys.argv))
flog = open(logfile,'w')
perf = time.perf_counter()

if len(sys.argv)<2:
    log('SYNTAX ERROR', 0)
    helpprt()
    sys.exit()
else:
    foldervideo = os.path.normpath(sys.argv[1])
    if foldervideo[-1] != "/": foldervideo = foldervideo + "/"
    if not(os.path.exists(foldervideo)):
        log(txtred + 'Error: ' + foldervideo + ' does not exists' + txtnocolor, 0)
        sys.exit()

    folderimg = os.path.normpath(sys.argv[2])
    if folderimg[-1] != "/": folderimg = folderimg + "/"
    if not(os.path.exists(folderimg)):
        os.makedirs(folderimg)

    tmp = '/tmp/' + pid + '/'
    for i in sys.argv[3:]:
      if i[:3] == '-v=': debug = int(i[3:])
      if i[:3] == '-f=': fpsn = float(i[3:])
      if i[:9] == '-threads=': threads = int(i[9:])
      if i[:5] == '-tmp=':
        if i[-1] == '/':
          tmp = i[5:] + pid + '/'
        else:
          tmp = i[5:] + '/' + pid + '/'
      if i == '-p': parallel = True
      if i == '-c':
        clean = True
        fast = False
      if i == '-fastclean':
        clean = True
        fast = True
      if i == '-moved': moved = True
      if i == '-nc': copyright = 12

    if not(os.path.exists('log')):
      os.makedirs('log')
    #os.makedirs(tmp)
    MinJpgCount = 5 / fpsn

    helpprt()
    log('foldervideo : ' + foldervideo, 5)
    log('folderimg : ' + folderimg, 5)
    log('fps : ' + str(fpsn), 5)
    log('nb args : ' + str(len(sys.argv)-1), 5)
    log('abspath' + os.path.abspath(foldervideo + '..'), 5)
    log('basename' + os.path.basename(foldervideo), 5)
    log('dirname' + os.path.dirname(foldervideo), 5)
    log('tmp = ' + tmp, 5)
    log('debug = ' + str(debug), 5)
    
    if fast:
      log('fastclean = ' + str(fast), 0)
    else:
      if clean:
        log('clean = ' + str(clean), 0)
      else:
        log('parallel = ' + str(parallel), 0)
    log('', 0)

    #must use Manager queue here, or will not work
    manager = multiprocessing.Manager()
    queue = manager.Queue()    
    pool = multiprocessing.Pool(threads)
    #put listener to work first
    watcher = pool.apply_async(listener, (queue,))
    jobs = []

    if (clean == parallel):
        log('SYNTAX ERROR : clean and parallel flag cannot be use on same time but one is mandatory since version g.', 0)
        exit()
    if clean:
        log(txtgreen + 'CLEAN MODE : ' + txtnocolor + 'Will remove unfinished work and rearange images to start over. DO NOT launch other instances before it is finished.', 0)
    if parallel:
        log(txtgreen + 'PARALLEL MODE : ' + txtnocolor + 'You can use other instances on other terminals or other computer. NONE can be a Clean instance or you will have inconsistencies.', 0)

    #Step 1: Delete obsolete images
    log('', 0)
    log('************************************************************************************', 0)
    if debug:
      log(' ' + txtgreen + 'Step 1: ' + txtnocolor + 'Delete obsolete images an move to align to ' + foldervideo, 0)
    else:
      log(' ' + txtgreen + 'Step 1: ' + txtnocolor + 'Delete obsolete images from ' + foldervideo, 0)
    log('************************************************************************************', 0)

    BoucleCount(foldervideo, folderimg, level)

    if clean:
        # Test of source with same name
        SameSrcNames()
        if renamed:
          # If samename renamed some files then reload source files
          cpttodo = 0
          srclst = []
          BoucleCount(foldervideo, folderimg, level)
                
        srclst = sorted(srclst, key=sortoccurence)
        BoucleSupp('')

    #Step 2: Create missing images
    log('', 0)
    log('************************************************************************************', 0)
    if debug:
      log(' ' + txtgreen + 'Step 2: ' + txtnocolor + 'Remove unfinished images for ' + foldervideo, 0)
    else:
      log(' ' + txtgreen + 'Step 2: ' + txtnocolor + 'Create missing images for ' + foldervideo, 0)
    log('************************************************************************************', 0)

    BoucleFichiers(foldervideo, folderimg, level)

    # collect results from the workers through the pool result queue
    
    while jobcounter < len(jobs):
      log('Scan folder finished. Start 1 thread.', 1)      
      jobs[jobcounter].get()
      jobcounter = jobcounter + 1

    #now we are done, kill the listener
    queue.put('kill')
    pool.close()
    pool.join()
      

log('************************************************************************************', 0)
log('* 1parse ' + foldervideo + ' ' + folderimg + txtgreen + ' DONE: ' + str(cptdone) + ' / ' + str(cpttodo) + txtnocolor, 0)
log('************************************************************************************', 0)
log('', 0)
flog.close
