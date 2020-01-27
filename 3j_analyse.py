#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#TO DO


import sys
import os
import subprocess
import shutil
import time
from PIL import Image
import numpy
import gmpy2
import multiprocessing

#Declarations
debug = 1
threads = 2
copyright = 1
maxdiff = 20
hdmaxdiff = 100
hdquality = 5
threshold = 1
thresholduw = 2
ctrlref = True
fake = False
skiphd = False
contuwfp = True
env = 'prd'
pid = str(time.time())
foutput = ''
unwanted = []
unwanted0 = []
srclst = []
srclst2 = []
imglst = []
logfile = 'log/3analyse.' + pid + '.log'
tmp = '/tmp/'
cptsrc = 0
txtgreen = '\033[0;32m'
txterr = '\033[0;33m'
txtnocolor = '\033[0m'
lstfmt = ['.MP4','.AVI','.MOV','.M4V','.VOB','.MPG','.MPEG','.MKV','.WMV','.ASF','.FLV','.RM','.OGM','.M2TS','.RMVB']

#log messages to log file and to screen
def log(s='', threshold=1):
    flog.write(s + '\n')
    if debug >= threshold:
        print(s)

def duration(d, dat=True):
    h = int(d // 3600)
    m = int((d - 3600*h) // 60)
    s = d - 3600*h - 60*m
    if d > 3600: r = '{:02d}'.format(h) + ' h ' + '{:02d}'.format(m)
    elif d > 60: r = '{:02d}'.format(m) + ' mn ' + '{:02.0f}'.format(s)
    else: r = '{:02.3f}'.format(s) + ' s'
    if dat:
      #tuple = time.localtime(time.time())
      #r = txtgreen + str(tuple[6]) + '/' + str(tuple[1]) + '/' + str(tuple[2]) + ' ' + str(tuple[3]) + ':' + str(tuple[4]) + ' ' + str(tuple[5]) + txtnocolor + ' ' + r
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

def TempName(line):
    pend = len(line) - 1
    while (pend > 0) and (line[pend] != '/'):
      pend = pend - 1
    if pend == 0:
      r = line
    else:
      pbeg = pend - 1
      while (pbeg >= 0) and (line[pbeg] != '/'):
        pbeg = pbeg - 1
        r = line[pbeg+1:pend-1] + "_" + line[pend+1:]
    return r

#Give the name of a file by removing the forder reference
def PathName(fullname):
    k = len(fullname) - 1
    while fullname[k] != '/': k = k - 1
    return str(fullname[:k])

def source(line):
    #Identify the source video file of this image
    k = len(line) - 1
    while line[k] != "/": k = k - 1
    return foldervideo + line[len(folderimg):k]

def newimage(line, full=False):
  # imglst[[name,path]]
  pend = len(line) - 1
  while line[pend] != '/': pend = pend - 1
  name = line[:pend]
  min = 0
  max = len(imglst) - 1
  r = '-'
  if imglst[min][0] == name: r = imglst[min][1]
  if imglst[max][0] == name: r = imglst[max][1]
  while r == '-':
    if (max - min) < 2: r = ''
    idx = (min + max) // 2
    if imglst[idx][0] == name: r = imglst[idx][1]
    if imglst[idx][0] > name: max = idx
    else: min = idx
  if full:
    r = r + line[pend:]
  return r

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
    tmpfile = tmp + TempName(file)

    if os.path.splitext(file)[1] == '.jpg':
      if quality == 1:
          s = 'convert "' + file + '"[160x160] -modulate 100,0 -blur 3x99 -normalize -equalize -resize 16x16 -threshold 50% "' + tmpfile +'"'
      if quality == 2:
          s = 'convert "' + file + '"[160x160] -modulate 100,0 -blur 3x99 -normalize -equalize -resize 28x16 -threshold 50% -crop 16x16+8+0 +repage "' + tmpfile +'"'
      if quality == 3:
          s = 'convert "' + file + '"[320x180] -crop 200x160+60+10 +repage -modulate 100,0 -blur 3x99 -normalize -equalize -resize 50x40 -threshold 50% "' + tmpfile +'"'
      if quality == 4:
          #s = 'convert "' + file + '"[320x180] -crop 200x120+60+30 +repage -modulate 100,0 -blur 3x99 -normalize -equalize -resize 60x40 -threshold 50% "' + tmpfile +'"'
          s = 'convert "' + file + '"[320x180] -gravity Center -crop 210x128+0-4 +repage -modulate 100,0 -blur 3x99 -normalize -equalize -resize 84x51 -threshold 50% "' + tmpfile +'"'
      if quality == 5:
          s = 'convert "' + file + '"[320x180] -gravity Center -crop 210x128+0-4 +repage -blur 3x99 -normalize -equalize -resize 84x51 -level 50%,50% "' + tmpfile +'"'

      log('Fingerprint : ' + s, 3)
      p=subprocess.Popen(s, stdout=subprocess.PIPE, shell=True)
      (output, err) = p.communicate()
      if err == None:
        if os.path.exists(tmpfile):
          im = Image.open(tmpfile)
          img = numpy.asarray(im)
          key = '0b'

          if quality == 5:
            for i in range(img.shape[0]):
              for j in range(img.shape[1]):
                for k in range(0,3):
                  if (img[i,j][k] < 128):
                      key = key + '0'
                  else:
                      key = key +'1'
            result = int(key,2)
          else:
            for i in range(img.shape[0]):
              for j in range(img.shape[1]):

                if (img[i,j] < 128):
                    key = key + '0'
                else:
                    key = key +'1'
            result = int(key,2)
          if display:
            log('quality = ' + str(quality))
            #log(key)
            if quality == 5:
              for i in range(img.shape[0]):
                s = ''
                for j in range(img.shape[1]):
                  if (img[i,j][0] < 128):
                      s = s + ' '
                  else:
                      s = s + txterr + 'X'
                  if (img[i,j][1] < 128):
                      s = s + ' '
                  else:
                      s = s + txtgreen + 'X'
                  if (img[i,j][2] < 128):
                      s = s + ' '
                  else:
                      s = s + txtnocolor + 'X'
                log(s + txtnocolor)
            else:
              for i in range(img.shape[0]):
                s = ''
                for j in range(img.shape[1]):
                  if (img[i,j] < 128):
                      s = s + '.'
                  else:
                      s = s +'X'
                log(s)
          else:
            os.remove(tmpfile)
        else:
          log(time.perf_counter() - perf + ' - ' + txterr + 'ERROR' + txtnocolor + ' in calcfp - key file generated but not exist: ' + tmpfile + ' (from ' + file + ')', 1)
  else:
    log(file + ' not found')
  return result

def loadunwanted(folder, action):
    global unwanted
    global unwanted0
    global uwpair

    if not(os.path.isdir(folder)):
      os.mkdir(folder, mode=0o777)

    if action == 'load':
        uwpair = []
        log(duration(time.perf_counter() - perf) + ' - Loading list of images to exclude from search from ' + folder, 1)
        for file in os.listdir(folder):
            if (os.path.splitext(file)[1] == '.jpg'):
                if (os.path.exists(folder + 'unwantedimages.fp')) and (os.path.getmtime(folder + 'unwantedimages.fp') < os.path.getatime(folder + file)):
                    log('Rebuilt unwantedimages.fp due to recent files added.', 3)
                    log(file + ' : ', 3)
                    log(str(os.path.getatime    (folder + file)) + ' - ' + str(time.localtime(os.path.getatime(folder + file))), 3)
                    log(str(os.path.getmtime    (folder + file)) + ' - ' + str(time.localtime(os.path.getmtime(folder + file))), 3)
                    log(str(os.path.getctime    (folder + file)) + ' - ' + str(time.localtime(os.path.getctime(folder + file))), 3)
                    log('unwantedimages.fp : ' + str(os.path.getmtime(folder + 'unwantedimages.fp')) + ' - ' + str(time.localtime(os.path.getmtime(folder + 'unwantedimages.fp'))), 3)
                    log('---', 3)
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
                unwanted0.append(gmpy2.hamdist(int(key[:-1]),0))
            f.close

        else:
            log(duration(time.perf_counter() - perf) + ' - Cache unwantedimages.fp to rebuild. Around 5 files per second.')
            for file in os.listdir(folder):
                if (os.path.splitext(file)[1] == '.jpg'):
                    key = calcfp(folder + file, 1)
                    if key in unwanted:
                        log(str(key) + ' : Other image with same key. Remove file from unwanted.', 1)
                        log(str(key) + ' : Other image with same key. Remove ' + folder + file, 4)
                        os.remove(folder + file)
                    else:
                        unwanted.append(key)
                        unwanted0.append(gmpy2.hamdist(key,0))
                        log(str(key) + ' added in unwanted.', 3)

            f = open(folder + 'unwantedimages.fp', 'w')
            for key in unwanted:
                f.write(str(key) + '\n')
            f.close

        log(duration(time.perf_counter() - perf) + ' - ' + str(len(unwanted)) + ' unwanted images fingerprinted and ' + str(len(uwpair)) + ' unwanted pairs of sources.')

    if action == 'ctrl':
        for sl in srclst:
            srclst2.append(sl[0])
        for file in os.listdir(folder):
            if (os.path.splitext(file)[1] == '.txt'):
                f = open(folder + file, 'r')
                nok = False
                for line in f:
                    if line[:5] == 'pair=':
                        seekstr = line[5:-1]
                        if seekstr not in srclst2:
                            nok = True
                            log(line[5:-1] +' is not in source list anymore,', 1)
                f.close
                if nok:
                    os.remove(folder + '/' + file)
                    log('removing ' + file, 1)

def LoadSources(folderv):
    global cptsrc
    global srclst

    if os.path.isdir(folderv):
        if folderv[-1] != "/":
          folderv = folderv + "/"
        for file in os.listdir(folderv):
            ext = os.path.splitext(file)[1]
            if os.path.isdir(folderv+file):
                LoadSources(folderv+file)
            elif ext.upper() in lstfmt:
                cptsrc = cptsrc + 1
                srclst.append([file, folderv])
    if folderv == foldervideo:
      log(duration(time.perf_counter() - perf) + ' - ' + str(len(srclst)) + ' loaded sources files.')

def LoadImages(folder):
    global imglst

    for entry in os.scandir(folder):
      if entry.is_dir():
        ext = os.path.splitext(entry)[1]
        if ext.upper() in lstfmt:
          imglst.append([entry.name, entry.path])
        else:
          LoadImages(entry.path)
    if folder == folderimg:
      log(duration(time.perf_counter() - perf) + ' - ' + str(len(imglst)) + ' loaded images folders.')

def mp1_ImagesControl(mpinput):
  def mpinternal_newimage(line):
    pend = len(line) - 1
    while line[pend] != '/': pend = pend - 1
    name = line[:pend]
    min = 0
    max = len(imglst) - 1
    r = '-'
    if imglst[min][0] == name: r = imglst[min][1]
    if imglst[max][0] == name: r = imglst[max][1]
    while r == '-':
      if (max - min) < 2: r = ''
      idx = (min + max) // 2
      if imglst[idx][0] == name: r = imglst[idx][1]
      if imglst[idx][0] > name: max = idx
      else: min = idx
    return r

  result = []
  nbstill = 0
  nbsrcnok = 0
  nbunwant = 0
  nbunwantth = 0
  nbnotfound = 0
  (records,srclst2,imglst,unwanted,nbline) = mpinput
  for rec in records:
    (line2, line4, key1, key2, similarity) = rec
    src1 = MidName(line2[5:], True)
    src2 = MidName(line4[5:], True)
    ok = False
    if (src1 == src2): nbstill = nbstill + 1
    else:
      if (src1 not in srclst2): nbsrcnok = nbsrcnok + 1
      else:
        if (src2 not in srclst2): nbsrcnok = nbsrcnok + 1
        else:
          if (key1 in unwanted) or (key2 in unwanted): nbunwant = nbunwant + 1
          else:
            ok = True
            if ok and (thresholduw > 0):
              iuw = 0
              nuw = len(unwanted)
              while ok and (iuw < nuw):
                ok = (gmpy2.hamdist(key1,unwanted[iuw]) > thresholduw) and (gmpy2.hamdist(key2,unwanted[iuw]) > thresholduw)
                iuw = iuw + 1
              if not(ok): nbunwantth = nbunwantth + 1
    if ok and not (os.path.exists(line2[5:])):
      if not (os.path.exists(mpinternal_newimage(line2[5:]) + '/' + ShortName(line2[5:]))):
        ok = False
        nbnotfound = nbnotfound + 1
    if ok and not (os.path.exists(line4[5:])):
      if not (os.path.exists(mpinternal_newimage(line4[5:]) + '/' + ShortName(line4[5:]))):
        ok = False
        nbnotfound = nbnotfound + 1
    if ok:
      if src1 < src2: setvideo = [src1,src2]
      else: setvideo = [src2,src1]
      setimg=[MidName(line2[5:]),MidName(line4[5:])]
      setkey=[key1,key2]
      result.append([1, setvideo, setimg, [], setkey, similarity])
  if debug > 1: print('.', end='', flush=True)
  return [result,nbstill, nbsrcnok, nbunwant, nbunwantth, nbnotfound]

def mp2_SourceControl(mpinput):
  (records, named, srclst) = mpinput
  result = []
  rejthr = 0
  rejref = 0
  rejimg = 0
  rejdel = 0
  for rec in records:
    # resultsetvideo.append([1, setvideo, setimg, setprt])
    keep = (rec[0] >= threshold)
    if not(keep):
      rejthr = rejthr + 1
    if keep and ctrlref:
      for j in range(len(rec[1])):
        if rec[1][j] in named:
          log('Rejected cause ' + rec[1][j] + ' previously referenced.', 3)
          keep = False
      if not(keep):
        rejref = rejref + 1
    if keep:
      images = sorted(rec[2])
      rec[2] = []
      prev = ''
      for j in range(len(images)):
        if prev != images[j]:
          rec[2].append(images[j])
          prev = images[j]
      n = threshold
      prev = ''
      for j in range(len(rec[2])):
        if prev == PathName(rec[2][j]):
          n = n + 1
        else:
          if n < threshold:
            #log('Rejected cause nb images < ' + str(threshold) + ' for one source :', 3)
            keep = False
          prev = PathName(rec[2][j])
          n = 1
      if n < threshold:
        #log('Rejected cause nb images < ' + str(threshold) + ' for one source :', 3)
        keep = False
      if not(keep):
        rejimg = rejimg + 1
    if keep:
      keep = False
      for srcelt in srclst:
        if srcelt[0] == rec[1][0]:
          keep = True
      if keep:
        keep = False
        for srcelt in srclst:
          if srcelt[0] == rec[1][1]:
            keep = True
      if not(keep):
        rejdel = rejdel + 1
        log('Deleted : ' + rec[1][1], 3)
    if keep:
      for j in range(len(rec[1])):
        named.append(rec[1][j])
      result.append(rec)
  return([result, rejthr, rejref, rejimg, rejdel])

def mp3_HD_ReadCache(mpinput):
  def mpinternal_newimage(line):
    pend = len(line) - 1
    while line[pend] != '/':
      pend = pend - 1
    name = line[:pend]
    short = line[pend+1:]
    min = 0
    max = len(imglst) - 1
    r = '-'
    if imglst[min][0] == name: r = imglst[min][1] + '/' + short
    if imglst[max][0] == name: r = imglst[max][1] + '/' + short
    while r == '-':
      if (max - min) < 2:
        r = ''
        log(txterr + 'ERROR' + txtnocolor + ' Not found. min max = ' + str(min) + ', ' + str(max) + ' - ' + imglst[idx][0] + ' =? ' + name)
      idx = (min + max) // 2
      if imglst[idx][0] == name: r = imglst[idx][1] + '/' + short
      if imglst[idx][0] > name: max = idx
      else: min = idx
    return r

  def mp3_hdkseek(name):
    min = 0
    max = len(hdcacheNames) - 1
    if max < 0:
      res = -3
    else:
      res = -2
      if hdcacheNames[min] == name: res = min
      if hdcacheNames[max] == name: res = max
      while res == -2:
        if (max - min) < 2: res = -1
        idx = (min + max) // 2
        if hdcacheNames[idx] == name: res = idx
        if hdcacheNames[idx] > name: max = idx
        else: min = idx
    #r=-3 error; -2 running; -1 not found; >=0 position
    return res

  (rs2, hdcacheNames, imglst) = mpinput
  result = []
  for recimg in rs2:
    if mp3_hdkseek(recimg) < 0:
    #if recimg not in hdcacheNames:
      result.append(mpinternal_newimage(recimg))
  if debug > 1: print('.', end='', flush=True)
  return result

def mp4_HD_WriteCache(mpinput):
  def mpinternal_calcfp(file, quality):
    result = -1
    if os.path.exists(file):
      tmpfile = tmp + TempName(file)
      if os.path.splitext(file)[1] == '.jpg':
        if quality == 1:
            s = 'convert "' + file + '"[160x160] -modulate 100,0 -blur 3x99 -normalize -equalize -resize 16x16 -threshold 50% "' + tmpfile +'"'
        if quality == 2:
            s = 'convert "' + file + '"[160x160] -modulate 100,0 -blur 3x99 -normalize -equalize -resize 28x16 -threshold 50% -crop 16x16+8+0 +repage "' + tmpfile +'"'
        if quality == 3:
            s = 'convert "' + file + '"[320x180] -crop 200x160+60+10 +repage -modulate 100,0 -blur 3x99 -normalize -equalize -resize 50x40 -threshold 50% "' + tmpfile +'"'
        if quality == 4:
            s = 'convert "' + file + '"[320x180] -gravity Center -crop 210x128+0-4 +repage -modulate 100,0 -blur 3x99 -normalize -equalize -resize 84x51 -threshold 50% "' + tmpfile +'"'
        if quality == 5:
            s = 'convert "' + file + '"[320x180] -colorspace sRGB -type truecolor -gravity Center -crop 210x128+0-4 +repage -blur 3x99 -normalize -equalize -resize 84x51 -level 50%,50% "' + tmpfile +'"'
        log(duration(time.perf_counter() - perf) + ' - mp Fingerprint : ' + s, 3)
        p=subprocess.Popen(s, stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()
        if err == None:
          if os.path.exists(tmpfile):
            result = -2
            cont = True
            try:
              im = Image.open(tmpfile)
            except:
              log(duration(time.perf_counter() - perf) + ' - ' + txterr + 'ERROR' + txtnocolor + ': ' + tmpfile + ' not found.', 3)
              cont = False
            if cont:
              img = numpy.asarray(im)
              im.close
              key = '0b'
              if (img.shape[0] < 16) or (img.shape[1] < 16):
                log(duration(time.perf_counter() - perf) + ' - ' + txterr + 'ERROR' + txtnocolor + ': ' + tmpfile + ' is sized ' + str(img.shape[0]) + 'x' + str(img.shape[1]) + '. From ' + file)
                result = -2
              else:
                if quality == 5:
                  for i in range(img.shape[0]):
                    for j in range(img.shape[1]):
                      for k in range(0,3):
                        if (img[i,j][k] < 128):
                            key = key + '0'
                        else:
                            key = key +'1'
                else:
                  for i in range(img.shape[0]):
                    for j in range(img.shape[1]):
                      if (img[i,j] < 128):
                          key = key + '0'
                      else:
                          key = key +'1'
                result = int(key,2)
              try:
                os.remove(tmpfile)
              except:
                log(duration(time.perf_counter() - perf) + ' - ' + txterr + 'WARNING' + txtnocolor + ' removing ' + tmpfile, 3)
          else:
            log(duration(time.perf_counter() - perf) + ' - ' + txterr + 'ERROR' + txtnocolor + ' skipped HD computation for ' + file + '. Relaunch when finished to complete.', 3)
            #log(output)
    return result

  mpresult = []
  mpkey = mpinternal_calcfp(mpinput, hdquality)
  if mpkey > -1:
    mpresult.append([MidName(mpinput), mpkey])
  #print('.', end='', flush=True)
  return mpresult

def mp5_HD_DistanceControl(mpinput):
  def mpinternal_source(line):
    #Identify the source video file of this image
    k = len(line) - 1
    while line[k] != "/": k = k - 1
    return foldervideo + line[len(folderimg):k]

  def hdkseek(name):
    min = 0
    max = len(hdcache) - 1
    if max < 0:
      res = -2
    else:
      res = -1
      if hdcache[min][0] == name: res = hdcache[min][1]
      if hdcache[max][0] == name: res = hdcache[max][1]
      while res == -1:
        if (max - min) < 2: res = -2
        idx = (min + max) // 2
        if hdcache[idx][0] == name: res = hdcache[idx][1]
        if hdcache[idx][0] > name: max = idx
        else: min = idx
    return res

  (rsvlst,hdcache) = mpinput
  result = []
  for elt in rsvlst:
    (i,rsvuniti) = elt
    hdkey  = []
    for j in range(len(rsvuniti[2])):
      hdk = hdkseek(rsvuniti[2][j])
      # hdk = ['file',key]
      if hdk < 0:
        log(duration(time.perf_counter() - perf) + ' - ' + txterr + 'ERROR' + txtnocolor + ': HD fingerprint does not exist: ' + rsvuniti[2][j], 1)
      else:
        #             sourcepathandfilename           imageshortfilename key   toto.mp4
        hdkey.append([mpinternal_source(rsvuniti[2][j]), rsvuniti[2][j], hdk, PathName(rsvuniti[2][j])])

    hdbest = []
    for j in range(0, len(hdkey)):
      hddupe = []
      for k in range(j+1, len(hdkey)):
        if hdkey[j][3] != hdkey[k][3]:  # if images j and k refers to different sources
          if (hdkey[j][2] != hdkey[k][2]):
            #                    distance                                  indexhdkey
            hddupe.append([gmpy2.hamdist(int(hdkey[j][2]),int(hdkey[k][2])), k])
          else:
            hddupe.append([0, k])
      hddupe = sorted(hddupe, key=sortoccurence)
      # distance, img1, img2
      if (hddupe != []):
        # hdbest         distance      imgname      imgname
        hdbest.append([hddupe[0][0], hdkey[j][1], hdkey[hddupe[0][1]][1]])
    if (hdbest != []):
      hdbest = sorted(hdbest, key=sortoccurence)
      j = 0
      s = ''
      hdbest2 = []
      while (j < len(hdbest)) and (hdbest[j][0] <= hdmaxdiff):
        s = s + str(hdbest[j][0]) + ' of distance -img=''' + hdbest[j][1] + ''' -img=''' + hdbest[j][2] + '\n'
        hdbest2.append(hdbest[j])
        j = j + 1
      if (j < threshold):
        log(duration(time.perf_counter() - perf) + ' - Resultset rejected after HD control. Best distances = ' + s, 3)
        log(rsvuniti[1][0], 3)
        log(rsvuniti[1][1], 3)
      else:
        result.append([i,hdbest2])
        log(duration(time.perf_counter() - perf) + ' - Resultset keep after HD control. Best distances = ' + s, 3)
        log(rsvuniti[1][0], 3)
        log(rsvuniti[1][1], 3)
        if debug > 2:
          print(hdbest)

  if debug > 1: print('.', end='', flush=True)
  return result

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
    log('3rs step of duplicate video finder. Exploit result found by previous program, make aditionnal controls based on user parameters and', copyright)
    log('copy duplicates in an analyse folder for manual action.', copyright)
    log('It is folder agnostic so if video files have moved its ok, but image folder and source folder have to be aligned. Then relaunching the', copyright)
    log('1f_analyse -c followed by 1f_analyse -p is a good practice.', copyright)
    log('', copyright)
    log('SYNTAX : 3analyse foldersrc folderimg findimagedupesresult [options]', copyright)
    log('-v=n           verbosity. Default=1', copyright)
    log('-threads=n     number of threads to use. Huge RAM usage. Default=2', copyright)
    log('-t=n           minimum number of similar images to declare a pair of source as duplicate.', copyright)
    log('-tu=n          similarity of images vs unwanted to declare as unwanted.', copyright)
    log('-maxdiff=n     restrict results of findimagedupesresult on similarity < maxdiff', copyright)
    log('-hdq=n         with n=2 fast 28x16, 3 (previous default) 50x40 little crop, 4 84x51 more crop, 5 84x51x3 colors (default)', copyright)
    log('                 5 is suggested because there is far less false positives. The CPU impact is mitigated by multithreading.', copyright)
    log('-hdmaxdiff=n   recalculate high def 50x40 similarity keys to filter final resultset.', copyright)
    log('                 Optimum range to test are : for hdq=1: 5-10, hdq=2: 15-30, hdq=3: 80-120, hdq=4: 150-300, hdq=5: 500-1500', copyright)
    log('-skiphd        avoid the 2nd control of HD keys. Faster but more false positives.', copyright)
    log('-out=file      output a new findimagedupesresult file without unwanted images to speed up next runs.', copyright)
    log('-tmp=file      to change from default /tmp.', copyright)
    log('-outhd=file    cache file that keep HDdistance between 2 images.', copyright)
    log('-ctrlref=False Will accept multiple occurence of a source in different sets. Risk of erasing both elements. Performance and storage hit.', copyright)
    log('-fake          Will not copy source and image files in analyse folder.', copyright)
    log('-uwfp          Stop after refreshing fingerprint cache of unwanted images.', copyright)
    log('', copyright)
    log('Other usage to check individual images and challenge the HD algorithm :', copyright)
    log('-img=file      Source image to test. Temp image will be keep in /tmp folder.', copyright)
    log('usage :', copyright)
    log('3j_analyse -img=./images/video1.mp4/img0001.jpg -img=./images/video2.mp4/img0009.jpg', copyright)
    log('', 0)


#main
perf = time.perf_counter()
flog = open(logfile,'w')

log('************************************************************************************')
#read arguments and conform them
log('Video DeDup : find video duplicates')
log('Copyright (C) 2020  Pierre Crette')
log('')

if (sys.argv[1][:5] == '-img='):
    debug = 12
    q = 5
    file=sys.argv[1][5:]
    k1 = calcfp(file, q, True)
    log('key1 = ' + str(k1))
    if (len(sys.argv) > 1):
      if (sys.argv[2][:5] == '-img='):
        file=sys.argv[2][5:]
        k2 = calcfp(file, q, True)
        log('key2 = ' + str(k2))
        log('Distance = ' + str(gmpy2.hamdist(k1, k2)))

    log('Done. Temp image file in ' + tmp)

else:
    if len(sys.argv) < 3:
        log('SYNTAX ERROR:')
        helpprt()
        exit()

    foldervideo = os.path.normpath(sys.argv[1])
    if foldervideo[-1] != "/": foldervideo = foldervideo + "/"
    folderimgraw = os.path.normpath(sys.argv[2])
    if folderimgraw[-1] != "/":
        folderimg = folderimgraw + "/db/"
        folderana = folderimgraw + "/ana-" + env + "-not-saved/"
        foutputhd = folderimgraw + "/hddb_" + str(hdquality) + ".fp"
    else:
        folderimg = folderimgraw + "db/"
        folderana = folderimgraw + "ana-" + env + "-not-saved/"
        foutputhd = folderimgraw + "hddb_" + str(hdquality) + ".fp"
    fresultset = os.path.normpath(sys.argv[3])
    for i in sys.argv[3:]:
        if i[:3] == '-v=': debug  = int(i[3:])
        if i[:9] == '-threads=': threads  = int(i[9:])
        if i[:3] == '-t=': threshold  = int(i[3:])
        if i[:4] == '-tu=': thresholduw = int(i[4:])
        if i[:9] == '-maxdiff=' : maxdiff = int(i[9:])
        if i[:11] == '-hdmaxdiff=' : hdmaxdiff = int(i[11:])
        if i[:5] == '-hdq=' : hdquality = int(i[5:])
        if i[:5] == '-out=': foutput = i[5:]
        if i[:7] == '-hdout=': foutputhd = i[7:]
        if i[:5] == '-tmp=': tmp = i[5:]
        if i[:9] == '-ctrlref=': ctrlref = not(i[9:] == 'False')
        if i == '-fake': fake = True
        if i == '-uwfp': contuwfp = False
        if i == '-skiphd': skiphd = True
        if i == '-nc': copyright = 12
    if tmp[-1] != '/': tmp = tmp + '/'
    f = open(tmp + 'test', 'w')
    f.write('test')
    f.close

    helpprt()

    sshell = sys.argv[0] + ' ' + sys.argv[1] + ' ' + sys.argv[2] + ' ' + sys.argv[3] + ' -v=' + str(debug) + ' -threads=' + str(threads)
    sshell = sshell + ' -t=' + str(threshold)  + ' -tu=' + str(thresholduw) + ' -maxdiff=' + str(maxdiff) + ' -hdmaxdiff=' + str(hdmaxdiff)
    sshell = sshell + ' -hdq=' + str(hdquality) + ' -hdout=' + foutputhd +' -tmp=' + tmp + ' -ctrlref=' + str(ctrlref) + ' -out=' + foutput
    if fake:
      sshell = sshell + ' -fake'
    log(txtgreen + sshell + txtnocolor, 1)
    log('', 0)
    log (txtgreen + 'Consider double if at least ' + str(threshold) + ' pair of images are similar in the set.' + txtnocolor, 0)
    log('', 0)

    perf = time.perf_counter()

    log(duration(time.perf_counter() - perf) + ' - Create a ' + str(threads) + ' pool for multiprocessing.')
    #pool = multiprocessing.Pool(threads, None, [], 1)
    pool1 = multiprocessing.Pool(threads)
    pool2 = multiprocessing.Pool(threads)
    pool3 = multiprocessing.Pool(threads)
    pool4 = multiprocessing.Pool(threads)
    pool5 = multiprocessing.Pool(threads)

    loadunwanted(folderimg + 'unwanted/', 'load')

    if contuwfp:
        log(duration(time.perf_counter() - perf) + ' - Load current folder of each source...',0)
        LoadSources(foldervideo)
        LoadImages(folderimg)
        srclst = sorted(srclst, key=sortoccurence)
        imglst = sorted(imglst, key=sortoccurence)

        loadunwanted(folderimg + 'unwanted/', 'ctrl')

        prev = ''
        for i in range(len(srclst)):
          if srclst[i][0] == prev:
            log(duration(time.perf_counter() - perf) + ' - File ' + srclst[i][1] + srclst[i][0] + ' is referenced 2 times. Launch again 1_analyse.py.')
          prev = srclst[i][0]

        #Step 1: parse fresultset and create memory map
        resultsetvideo = []
        mpdata = []
        mpdatalist = []
        nbline = 0
        nbheap = 0
        nbstill = 0
        nbdist =0
        nbunwant = 0
        nbunwantth = 0
        nbuwpair = 0
        nbsrcnok = 0
        nbnotfound = 0

        f = open(fresultset, 'r')
        line1 = f.readline()[:-1]
        while line1:
          if (nbline % 1000000 == 0) or (nbline == 100000) or (nbline == 10000) or (nbline == 1000):
              log(duration(time.perf_counter() - perf) + ' - Loading file. Done: {:_}'.format(nbline), 2)
          nbline = nbline + 1
          # Read 6 lines
          line2 = f.readline()[:-1]
          line3 = f.readline()[:-1]
          line4 = f.readline()[:-1]
          line5 = f.readline()[:-1]
          line6 = f.readline()[:-1]
          # Controls structure
          if (line1[:5] != 'BEGIN') or (line1[7:18] != 'Similarity=') or (line6[:3] != 'END') or (line2[:5] != 'file=') or (line4[:5] != 'file=') or (line3[:4] != 'key=') or (line5[:4] != 'key='):
            log(duration(time.perf_counter() - perf) + ' - ' + txterr + 'ERROR' + txtnocolor + ' in the structure of the file. Found:')
            log(line1)
            log(line2)
            log(line3)
            log(line4)
            log(line5)
            log(line6)
            sys.exit(1)
          # Apply filters
          similarity = int(line1[18:])
          if (similarity >= maxdiff): nbdist = nbdist + 1
          else:
            key1 = int(line3[4:])
            key2 = int(line5[4:])
            mpdatalist.append([line2, line4, key1, key2, similarity])
            nbheap = nbheap + 1
            if (nbheap == 10000):
              mpdata.append([mpdatalist, srclst2, imglst, unwanted, nbline])
              mpdatalist = []
              nbheap = 0
          line1 = f.readline()[:-1]
        f.close()
        mpdata.append([mpdatalist, srclst2, imglst, unwanted, nbline])

        log(duration(time.perf_counter() - perf) + ' - {:_} records kept. Launching parrallel additional controls at images level.'.format(nbline - nbdist))
        results = pool1.imap_unordered(mp1_ImagesControl, mpdata, 5)
        pool1.close
        pool1.join

        for rr in results:
          for r in rr[0]:
            resultsetvideo.append(r)
          nbstill = nbstill + rr[1]
          nbsrcnok = nbsrcnok + rr[2]
          nbunwant = nbunwant + rr[3]
          nbunwantth = nbunwantth + rr[4]
          nbnotfound = nbnotfound + rr[5]

        worksize = len(resultsetvideo)
        if debug > 1: print()
        log(duration(time.perf_counter() - perf) + ' - STEP1 done. On {:_}'.format(nbline) + ' duplicates images, ' + txtgreen + \
            '{:_}'.format(worksize) + ' ({:_}%)'.format(100 * worksize // nbline) + txtnocolor +  ' dupes found. {:_}'.format(nbstill) + \
            ' stills rejected, {:_}'.format(nbunwant) + ' unwanted images,  {:_}'.format(nbunwantth) + \
            ' images distant from a unwanted <= ' + str(thresholduw) + ',  {:_}'.format(nbsrcnok) + ' source deleted or renamed,  {:_}'.format(nbnotfound) + \
            ' images not found and {:_}'.format(nbdist) + ' images with distance > ' + str(maxdiff), 1)
    #    log(duration(time.perf_counter() - perf) + ' - STEP1 done. On {:.{prec}f}'.format(nblines/6, prec=0) + ' duplicates images, ' + txtgreen + '{:_}'.format(worksize) + txtnocolor + \
    #        ' dupes found. {:_}'.format(nbstill) + ' stills rejected and {:_}'.format(nbunwant) + ' unwanted images and {:_}'.format(nbunwantth) + ' similar to uw images.', 1)

        if foutput != '':
          log(duration(time.perf_counter() - perf) + ' - Start writting output file to disk.', 1)
          resultsetvideo = sorted(resultsetvideo, key=sortimages)
          f = open(foutput, 'w')
          prev = ['','']
          nbdupes = 0
          for r in resultsetvideo:
              if prev == r[2]:
                log('Out dupe removed : ' + prev[0] + '; ' + prev[1], 3)
                nbdupes = nbdupes + 1
              else:
                prev = r[2]
                f.write('BEGIN. Similarity=' + str(r[5]) + '\n')
                f.write('file=' + r[2][0] + '\n')
                f.write('key=' + str(r[4][0]) + '\n')
                f.write('file=' + r[2][1] + '\n')
                f.write('key=' + str(r[4][1]) + '\n')
                f.write('END' + '\n')
          f.close
          log(duration(time.perf_counter() - perf) + ' - Output file written to disk : ' + foutput, 1)
          log(duration(time.perf_counter() - perf) + ' -    Removed duplicates pairs of images : ' + str(nbdupes), 1)

        if not(fake):
          #Step 2: clean files in multiple duplicates
          log('*********************************************************************', 1)
          log('* STEP 2 : CONTROLS AT SOURCE LEVEL AND HD COMPARE TO NARROW FILTER *', 1)
          log('*********************************************************************', 1)
          if worksize > 100000:
            log(txtgreen + sshell + txtnocolor, 1)

          #Sort by 1st source then group and count same duplicate sets.
          resultsetvideo = sorted(resultsetvideo, key=sortsources)

          log(duration(time.perf_counter() - perf) + ' - Sorted {:_}'.format(len(resultsetvideo)) + ' images dupes.', 0)
          log(duration(time.perf_counter() - perf) + ' - Grouping by pairs of sources and removing unwanted source')
          rsv = []
          prev = ['','']

          for i in range(len(resultsetvideo)):
            if prev == resultsetvideo[i]:
              log('Do nothing, complete duplicate.', 4)
            else:
              if resultsetvideo[i][1] in uwpair:
                nbuwpair = nbuwpair + 1
              else:
                if prev[1] != resultsetvideo[i][1]:
                  prev = resultsetvideo[i]
                  rsv.append(prev)
                else:
                  rsv[len(rsv)-1][0] = rsv[len(rsv)-1][0] + 1
                  for j in range(len(resultsetvideo[i][2])):
                      rsv[len(rsv)-1][2].append(resultsetvideo[i][2][j])
              # Remove duplicate source images
              tmprs = sorted(resultsetvideo[i][2])
              resultsetvideo[i][2] = []
              previ = ''
              for j in range(len(tmprs)):
                  if (previ != tmprs[j]):
                      previ = tmprs[j]
                      resultsetvideo[i][2].append(previ)
              tmprs = []

          log(duration(time.perf_counter() - perf) + ' - Grouped by source files from {:_}'.format(len(resultsetvideo)) + ' to {:_}'.format(len(rsv)) + ' sources dupes. {:_}'.format(nbuwpair) + ' unwanted pairs.', 0)
          log(duration(time.perf_counter() - perf) + ' - Controls at video level: count images similarities per source pair and check multiple referencing.')

          rsv = sorted(rsv, key=sortoccurence, reverse=True)

          named= []
          resultsetvideo = []
          log('Check occurence >= ' + str(threshold), 3)
          rejthr = 0
          rejref = 0
          rejdel = 0
          rejimg = 0
          mpdata = []
          mpdatalist = []

          #imglst = sorted(imglst, key=sortoccurence)

          for i in range(len(rsv)):
            mpdatalist.append(rsv[i])
            if (i % 1000 == 0) and (i > 0):
              mpdata.append([mpdatalist, named, srclst])
              mpdatalist = []
          if len(mpdatalist) > 0:
            mpdata.append([mpdatalist, named, srclst])
          results = pool2.imap_unordered(mp2_SourceControl, mpdata, 5)
          pool2.close
          pool2.join
          for rr in results:
            for r in rr[0]:
              resultsetvideo.append(r)
            rejthr = rejthr + rr[1]
            rejref = rejref + rr[2]
            rejdel = rejdel + rr[4]
            rejimg = rejimg + rr[3]

          log(duration(time.perf_counter() - perf) + ' - Controls restricted list from {:_}'.format(len(rsv)) + ' to ' + txtgreen + \
            '{:_}'.format(len(resultsetvideo)) + ' dupes.' + txtnocolor + ' {:_}'.format(rejthr) + ' rejected due to less than {:_}'.format(threshold) + \
            ' pair of duplicates, {:_}'.format(rejimg) + ' because less than {:_}'.format(threshold) + ' distinct images identified, {:_}'.format(rejref) + \
            ' previously references sources, {:_}'.format(rejdel) + ' deleted sources.', 1)

          if skiphd:
            log(duration(time.perf_counter() - perf) + ' - Skip HD control.', 1)
          else:
          #Calculate HD distance to limit resultset
            hdcache = []
            hdkey = -1
            if os.path.exists(foutputhd):
              f = open(foutputhd, 'r')
              for line in f:
                if line[:6] == 'hdkey=':
                  hdkey = line[6:-1]
                if line[:5] == 'file=':
                  if (len(line) > 22):
                      hdcache.append([line[5:-1], int(hdkey)])
              f.close
              log(duration(time.perf_counter() - perf) + ' - HD cache loaded with ' + str(len(hdcache)) + ' elements.', 1)
            else:
              if foutputhd != '':
                f = open(foutputhd, 'w')
                f.close

            # Complete HD fingerprint database
            hdcacheNames = []
            for c in hdcache:
              hdcacheNames.append(c[0])
            hdcacheNames = sorted(hdcacheNames)

            log(duration(time.perf_counter() - perf) + ' - Find missing HD fingerprints.', 2)
            mpdatalist = []
            for rs in resultsetvideo:
              for img in rs[2]:
                mpdatalist.append(img)
            mpdatalist = sorted(mpdatalist)
            tmprs = []
            prev = ''
            for r in mpdatalist:
              if r != prev:
                tmprs.append(r)
              prev = r
            log(duration(time.perf_counter() - perf) + ' - For {:_} images to compare in HD, '.format(len(mpdatalist)) + '{:_} uniques references to check for existing HD fingerprint or search current path.'.format(len(tmprs)), 1)

            mpdata = []
            mpdatalist = []
            cpt = 0
            for img in tmprs:
              mpdatalist.append(img)
              if len(mpdatalist) == 200:
                cpt = cpt + len(mpdatalist)
                mpdata.append([mpdatalist, hdcacheNames, imglst])
                mpdatalist = []
            if len(mpdatalist) > 0:
              cpt = cpt + len(mpdatalist)
              mpdata.append([mpdatalist, hdcacheNames, imglst])
            log(duration(time.perf_counter() - perf) + ' - Find missing HD fingerprints and search folder for {:_} images.'.format(cpt), 1)
            results = pool3.imap_unordered(mp3_HD_ReadCache, mpdata, 5)
            pool3.close
            pool3.join
            imagefilestoHDcache = []
            for rr in results:
              for r in rr:
                imagefilestoHDcache.append(r)

            if debug > 1: print()
            log(duration(time.perf_counter() - perf) + ' - ' + txtgreen + str(len(imagefilestoHDcache)) + txtnocolor + ' missing HD key to compute.', 1)
            if len(imagefilestoHDcache) > 0:
              chunk = 1000 * threads
              while len(imagefilestoHDcache) > 0:
                if len(imagefilestoHDcache) > chunk:
                  tmpimg = imagefilestoHDcache[:chunk]
                  imagefilestoHDcache = imagefilestoHDcache[chunk:]
                else:
                  tmpimg = imagefilestoHDcache
                  imagefilestoHDcache = []
                results = pool4.imap_unordered(mp4_HD_WriteCache, tmpimg)
                cpt = 0
                f = open(foutputhd, 'a')
                for rr in results:
                  for r in rr:
                    hdcache.append(r)
                    f.write('hdkey=' + str(r[1]) + '\n')
                    f.write('file=' + r[0] + '\n')
                    cpt = cpt + 1

              log(duration(time.perf_counter() - perf) + ' - Computation done. Now writing cache to disk.', 2)
              hdcache = sorted(hdcache, key=sortoccurence)
              f = open(foutputhd + '~', 'w')
              prev = []
              for hdc in hdcache:
                if hdc != prev:
                  f.write('hdkey=' + str(hdc[1]) + '\n')
                  f.write('file=' + hdc[0] + '\n')
                  prev = hdc
              f.close
              os.remove(foutputhd)
              os.rename(foutputhd + '~',foutputhd)
              log(duration(time.perf_counter() - perf) + ' - ' + txtgreen + str(cpt) + txtnocolor + ' new HD key added to cache ' + foutputhd, 1)

            pool4.close
            pool4.join
            log(duration(time.perf_counter() - perf) + ' - HD distances to compute.', 1)

            # HD comparison
            #f = open(foutputhd, 'r')
            mpdata = []
            mpdatalist = []

            for i in range(len(resultsetvideo)):
              mpdatalist.append([i,resultsetvideo[i]])
              if (i % 1000 == 0) and (i > 0):
                mpdata.append([mpdatalist,hdcache])
                mpdatalist = []
            if len(mpdatalist) > 0:
              mpdata.append([mpdatalist,hdcache])
            results = pool5.imap_unordered(mp5_HD_DistanceControl, mpdata, 5)
            pool5.close
            pool5.join
            rsv = []
            comment = []
            for rr in results:
              for r in rr:
                rsv.append(resultsetvideo[r[0]])
                comment.append(r[1])

            if debug > 1: print()
            log(duration(time.perf_counter() - perf) + ' - Limit to max HD distance done. From {:_}'.format(len(resultsetvideo)) + txtgreen + \
             ' to {:_}'.format(len(rsv)) + ' dupes.' + txtnocolor, 0)
            resultsetvideo = rsv

          #Step 3: create Analyse folder and copy all files in it
          log('*******************************************')
          log('*    STEP 3 : COPY FILES FOR ANALYSIS     *')
          log('*******************************************')
          if fake:
            log(txtgreen + 'Fake: Analyse folder not created.' + txtnocolor)
            for i in range(len(resultsetvideo)):
              log('resultsetvideo[' + str(i) + '] : occurences = ' + str(resultsetvideo[i][0]), 2)
              log('Sources :', 2)
              for j in range(len(resultsetvideo[i][1])): log(resultsetvideo[i][1][j], 2)
              log('Images :', 4)
              for j in range(len(resultsetvideo[i][2])): log(resultsetvideo[i][2][j], 4)
          else:
            log(txtgreen + sshell + txtnocolor, 1)
            if not(os.path.exists(folderana)):
              os.mkdir(folderana, mode=0o777)
            for j in range(len(resultsetvideo)):
              ok = True
              fld = folderana + str(j) + '/'
              x = resultsetvideo[j]
              if x[0] >= threshold:
                if os.path.exists(fld):
                  shutil.rmtree(fld)
                os.mkdir(fld, mode=0o777)

                #x[1] are Video source files
                for d in enumerate(x[1]):
                  patd1 = ''
                  for srcelt in srclst:
                    if srcelt[0] == d[1]:
                      patd1 = srcelt[1] + d[1]

                  log('Copy ' + patd1 + ' ' + fld + SlashToSpace(patd1, len(foldervideo)))
                  if ok and os.path.exists(patd1):
                    shutil.copy2(patd1, fld + SlashToSpace(patd1, len(foldervideo)))
                  else:
                    ok = False

                #x[2] are images files
                if ok:
                  f = open(fld + '/nb_match_' + str(x[0]) + '.' + str(j) + '.' + pid + '.txt','w')
                  f.write('To move in ' + folderimg + '/unwanted to remove this pair from future comparison :\n')
                  for d in enumerate(x[1]):
                    f.write('pair=' + d[1] + '\n')
                  f.write('#\n')
                  f.write('Similar images files :\n')
                  prev = ''
                  for d in enumerate(x[2]):
                    # log('prev = ' + prev, 4)
                    # log('d[1]    = ' + d[1], 4)
                    if d[1] != prev:
                      # log('d <> prev', 4)
                      # f.write(d[1] + '\n')
                      imgf = newimage(d[1]) + '/' + ShortName(d[1])
#                      if os.path.exists(imgf):
#                        shutil.copy2(imgf,fld + SlashToSpace(imgf, len(folderimg)))
#                      else:
#                        log(txterr + 'Not exist ' + txtnocolor + d[1] + txtgreen + ' -> ' + txtnocolor + imgf, 1)
                    prev = d[1]
                  for elt in comment[j]:
                    im1 = newimage(elt[1], True)
                    im2 = newimage(elt[2], True)
                    f.write(str(elt[0]) + ' of distance -img="' + im1 + '" -img="' + im2 + '"\n')
                    if os.path.exists(im1):
                      shutil.copy2(im1,fld + SlashToSpace(im1, len(folderimg)))
                    else:
                      log(txterr + 'Not exist ' + txtnocolor + im1, 1)
                    if os.path.exists(im2):
                      shutil.copy2(im2,fld + SlashToSpace(im2, len(folderimg)))
                    else:
                      log(txterr + 'Not exist ' + txtnocolor + im2, 1)
                  f.close
                else:
                  shutil.rmtree(fld)

#    if HDerrflag:
#      log(duration(time.perf_counter() - perf) + ' - ' + txterr + 'WARNING' + txtnocolor + ' Some HD computation failed due to system constraints, so the HD cache is not complete. Relaunch again to finish. Consider this result as correctt but partial.')
    log(duration(time.perf_counter() - perf) + ' - ' + txtgreen + 'Finished.' + txtnocolor, 1)
    log('', 0)
    log(txtgreen + sshell + txtnocolor, 1)
flog.close
