program compare;

{$mode objfpc}{$H+}
uses
{$ifdef unix}
  cthreads,
  cmem, // the c memory manager is on some systems much faster for multi-threading
  Classes,

{$endif}
  //Crt,
  SysUtils, IniFiles, Process, Math, DateUtils, SysCall;

{$OPTIMIZATION LEVEL3}


{
TODO :
Mask options
Other parameters missing ?
-skip to load only part of dataset and be able to compare last sources together in order to spare RAM.

v20190529 08.beta : log message update
v20190530 18 & 22.beta : count and display workload
v20190531 15.beta : log message update
v20190602 : count images and better display performance
}


const
  version: string = 'compare v20190602';

Type
    TMyThread = class(TThread)
    private
      FAFinished: boolean;
    public
      procedure Execute; override;
      property AFinished: boolean read FAFinished write FAFinished;
    end;

  TKey = array[0..3] of Qword;
  pFingerprint = ^TFingerprint;
  TFingerprint = record
    skey : string;
    key  : TKey;
    img  : string;
    next : pFingerprint;
    end;
  pSource = ^TSource;
  TSource = record
    midpath    : string;
    filename   : string;
    firstimage : pFingerprint;
    next       : pSource;
  end;

var
  folderimg, flogname: string;
  debug, threshold, copyright, mask, masksize : integer;
  script, maskmethod: string;
  i, filecount, imgcount, nbthreads: integer;
  p, param, pid, lbl: string;
  checkall: boolean;
  firstsource: pSource;
  debut: TDateTime;

  //testskey1, testskey2: string;
  //testkkey1, testkkey2: TKey;

//function TestTimeStamp: string;
//  var
//    st      : TSystemTime;
//    current : TDateTime;
//    dur     : Qword;
//    durts   : TTimeStamp;
//    sec, mn, hr: integer;
//    tmp     : string;
//  begin
//    dur := strtoint(testskey2);
//    writeln(dur);
//    sec := dur mod 60;
//    writeln('sec=' + inttostr(sec));
//    dur := (dur - sec) div 60;
//    writeln(dur);
//    mn  := dur mod 60;
//    writeln('mn=' + inttostr(mn));
//    dur := (dur - mn) div 60;
//    writeln(dur);
//    hr  := dur mod 24;
//    writeln('hr=' + inttostr(hr));
//    tmp := ' (' + inttostr(durts.Date) + 'j ' + rightstr('0'+inttostr(hr),2) + ':' + rightstr('0'+inttostr(mn),2) + '.' + rightstr('0'+inttostr(sec),2) + ') ';
//
//    DateTimeToSystemTime(current,st);
//    TestTimeStamp:=inttostr(st.year) + '/' + rightstr('0'+inttostr(st.month),2) + '/' + rightstr('0'+inttostr(st.Day),2) + ' ' + inttostr(st.Hour) + ':'
//    + rightstr('0'+inttostr(st.minute),2) + ':' + rightstr('0'+inttostr(st.Second),2)
//    + tmp;
//
//    writeln(TestTimeStamp);
//  end;

function TimeStamp: string;
var
  st      : TSystemTime;
  current : TDateTime;
  dur     : Qword;
  durts   : TTimeStamp;
  sec, mn, hr: integer;
  tmp     : string;
begin
  current := Now;
  durts := MSecsToTimeStamp(TimeStampToMSecs(DateTimeToTimeStamp(current)) - TimeStampToMSecs(DateTimeToTimeStamp(debut)));
  dur := durts.Time div 1000;
  sec := dur mod 60;
  dur := (dur - sec) div 60;
  mn  := dur mod 60;
  dur := (dur - mn) div 60;
  hr  := dur mod 24;
  tmp := ' (' + inttostr(durts.Date) + 'j ' + rightstr('0'+inttostr(hr),2) + ':' + rightstr('0'+inttostr(mn),2) + '.' + rightstr('0'+inttostr(sec),2) + ') ';

  DateTimeToSystemTime(current,st);
  TimeStamp:=inttostr(st.year) + '/' + rightstr('0'+inttostr(st.month),2) + '/' + rightstr('0'+inttostr(st.Day),2) + ' ' + inttostr(st.Hour) + ':'
  + rightstr('0'+inttostr(st.minute),2) + ':' + rightstr('0'+inttostr(st.Second),2)
  + tmp;
end;

procedure Log(s: string; verbose:integer; inthread: boolean);
var
  flog: TextFile;
  output: string;
begin
  output := TimeStamp + s;
  if flogname <> '' then begin
    if FileExists(flogname) then begin
      assignfile(flog, flogname);
      append(flog);
      writeln(flog, output);
      close(flog);
    end else begin
      if inthread then begin
         writeln('Error writing into LOG file.');
      end else begin
        assignfile(flog, flogname);
        rewrite(flog);
        writeln(flog, output);
        close(flog);
      end;
    end;
  end;
  if verbose <= debug then writeln(output);
end;

//procedure TestFichier;
//var
//  f: TextFile;
//  n: integer;
//begin
//  log('debut', 0, false);
//  assignfile(f, 'test1');
//  if not(FileExists('test1')) then begin
//    rewrite(f);
//    close(f);
//  end;
//  append(f);
//  for i:=1 to 1000000 do begin
//    writeln(f, 'ceci est un test sans refermer le fichier entre chaque ligne.');
//  end;
//
//  log('milieu', 0, false);
//  assignfile(f, 'test2');
//  if not(FileExists('test2')) then begin
//    rewrite(f);
//    close(f);
//  end;
//  for i:=1 to 1000000 do begin
//    assignfile(f, 'test2');
//    append(f);
//    writeln(f, 'ceci est un test sans refermer le fichier entre chaque ligne.');
//    close(f);
//  end;
//
//  log('fin', 0, false);
//end;

function ImageName(line, path: string): string;
var
  pend: integer;
begin
  pend := length(line) - 1;
  while (line[pend] <> '/') and (pend > 0) do begin
    dec(pend);
  end;
  ImageName := path + line[pend..length(line)];
end;

function EndName(path: string): string;
var
  pend, pbeg: integer;
begin
  pend := length(path) - 1;
  while (pend > 1) and (path[pend] <> '/') do dec(pend);
  pbeg := pend - 1;
  while (pbeg > 1) and (path[pbeg] <> '/') do dec(pbeg);
  EndName := path[pbeg+1..pend-1];
end;

function MidPath(path: string): string;
var
  p: integer;
begin
  p := length(path) - 1;
  while (p > 1) and (path[p] <> '/') do dec(p);
  dec(p);
  while (p > 1) and (path[p] <> '/') do dec(p);
  MidPath := copy(leftstr(path,p), length(folderimg)+1);
end;

//function inttobin(n: Qword): string;
//var
//  s: string;
//begin
//  if n mod 2 = 0 then s:='0'+s else s:='1'+s;
//  while n>1 do begin
//    n := n div 2;
//    if n mod 2 = 0 then s:='0'+s else s:='1'+s;
//  end;
//  inttobin := rightstr('000000000000000000000000000000000000000000000000000000000000000' + s, 64);
//end;

//debug
//function keytostr(k: TKey): string;
//begin
//  writeln(k[0]);
//  keytostr := inttobin(k[3]) + ' ' + inttobin(k[2]) + ' ' + inttobin(k[1]) + ' ' + inttobin(k[0]);
//end;

function keytobin(key: string): TKey;
var
  k,tmp: array[0..255] of byte;
  i, len, deb, cpt: byte;
  //sbin: string;
  tbin: TKey;
begin
  len   := length(key);
  deb   := 1;
  for i := 0 to 255 do tmp[i] := 0;
  for i := 1 to len do k[i] := strtoint(key[i]);

  //sbin := inttostr(k[len] mod 2);
  tmp[255] := k[len] mod 2;
  cpt := 254;

  while (deb < len) or (k[len] > 1) do begin
    for i := deb to len do begin
      if i<len then k[i+1] := k[i+1] + 10 * (k[i] mod 2);
      k[i] := k[i] div 2;
    end;
    if k[deb] = 0 then inc(deb);

    //if verbose then begin
    //  if length(sbin) mod 8 = 0 then sbin := ' ' + sbin;
    //  sbin := inttostr(k[len] mod 2) + sbin;
    //end;
    tmp[cpt] := k[len] mod 2;
    dec(cpt);
  end;

  //if verbose then writeln(sbin);

  for i:=0 to 3 do tbin[i] := 0;
  for i:=0 to 63 do begin
    tbin[3] := 2 * tbin[3] + tmp[i];
    tbin[2] := 2 * tbin[2] + tmp[i+64];
    tbin[1] := 2 * tbin[1] + tmp[i+128];
    tbin[0] := 2 * tbin[0] + tmp[i+192];
  end;

  //sbin := '';
  //for i := 0 to 3 do sbin := inttostr(tbin[i]) + ' ' + sbin;
  //writeln(sbin);

  keytobin := tbin;
end;

function iif(cond: boolean; msg: string): string;
begin
  if cond then iif:=msg else iif:='';
end;

//procedure TestMem;
//var
//  pt: pSource;
//  ptimg : pFingerprint;
//  tmps: string;
//  tmpkey: TKey;
//  cpt: integer;
//begin
//  pt   := firstsource;
//  cpt  := 0;
//  while (pt <> nil) do begin
//    inc(cpt);
//    log('Test of #' + inttostr(cpt) + ' / ' + inttostr(filecount) + pt^.midpath + pt^.filename, 1);
//    if (rightstr(pt^.filename,4)= '.mp4') or (rightstr(pt^.filename,4)='.avi') or (rightstr(pt^.filename,4)='.wmv') or (rightstr(pt^.filename,3)='.rm')
//      or (rightstr(pt^.filename,4)='.VOB') or (rightstr(pt^.filename,4)='.mkv') or (rightstr(pt^.filename,4)='.mpg') or (rightstr(pt^.filename,4)='.m4v')
//      or (rightstr(pt^.filename,4)='.mov') then begin
//      sleep(100);
//    end else begin
//      sleep(2000);
//    end;
//    if pt^.next = nil then log('  pt^.next = nil',1) else log('  pt^.next <> nil',1);
//    if cpt>454 then sleep(3000);
//    ptimg := pt^.firstimage;
//    while ptimg <> nil do begin
//      if rightstr(ptimg^.img,4) = '.jpg' then begin
//        write('.');
//      end else begin
//        writeln(ptimg^.img);
//        sleep(2000);
//      end;
//      tmps := ptimg^.img + ptimg^.skey;
//      tmpkey := ptimg^.key;
//      ptimg := ptimg^.next;
//    end;
//    writeln(';');
//    pt   := pt^.next;
//  end;
//  log('END OF TESTMEM  : OK', 1);
//  sleep(3000);
//end;

function FindPrevSource(mname: string): pSource;
var
  pt, prev: pSource;
begin
  //return nil if list empty or 1st
  //could be optimized by dichotomy or bubble sort
  pt   := firstsource;
  prev := nil;
  while (pt <> nil) and (mname > pt^.filename) do begin
    prev := pt;
    pt   := pt^.next;
  end;
  FindPrevSource := prev;
end;

procedure RecurseScan(fold: string; var filecount: integer);
var
  fullname, skey, simg: string;
  result: TRawByteSearchRec;
  f: TextFile;
  pt, prev: pFingerprint;
  ptsource, ptprevsource: pSource;

begin
  fold:=ExpandFileName(fold);
  if fold[length(fold)]='/' then fold := leftstr(fold,length(fold)-1);
  if fold <> folderimg + 'unwanted' then begin
    Log('Load fingerprints from ' + fold, 3, false);
    if (filecount mod 1000 = 0) and (filecount > 0) then
      log('Loaded ' + inttostr(filecount) + ' images folders...', 1, false);
    if FindFirst(fold + '/*', faAnyFile and faDirectory, result)=0 then begin
      repeat
        if (result.Name <> '.') and (result.Name <> '..') then
        begin
          If (result.Attr and faDirectory) = faDirectory then begin
            recursescan(fold + '/' + result.Name, filecount);
          end else begin
            if upcase(RightStr(result.Name, 3)) = '.FP' then begin
              fullname := fold + '/' + result.Name;
              inc(filecount);
              if fileexists(fullname) then begin
                assignfile(f, fullname);
                reset(f);
                new(ptsource);
                ptsource^.midpath  := MidPath(fullname);
                ptsource^.filename := EndName(fullname);
                ptprevsource       := FindPrevSource(ptsource^.filename);
                if ptprevsource = nil then begin
                  //element to add at the beginning of the list
                  ptsource^.next     := firstsource;
                  firstsource        := ptsource;
                end else begin
                  //element to add not at the begin
                  ptsource^.next     := ptprevsource^.next;
                  ptprevsource^.next := ptsource;
                end;

                prev := nil;
                while not eof(f) do begin
                  readln(f, skey);
                  if leftstr(skey,4) <> 'key=' then begin
                     Log('Error : in ' + fullname + ' Look for key= and found ' + skey, 0, false);
                     halt;
                  end;
                  readln(f, simg);
                  if leftstr(simg,5) <> 'file=' then begin
                     Log('Error : in ' + fullname + ' Look for file= and found ' + simg, 0, false);
                     halt;
                  end;

                  inc(imgcount);
                  new(pt);
                  pt^.skey   := rightstr(skey, length(skey)-4);
                  pt^.key    := keytobin(rightstr(skey, length(skey)-4));
                  pt^.img    := ImageName(simg, fold);
                  pt^.next   := nil;
                  if prev = nil then
                    ptsource^.firstimage := pt
                  else
                    prev^.next := pt;
                  prev       := pt;
                end;
                closefile(f);
                if prev = nil then begin
                  log('ERROR no images in ' + fullname, 2, false);
                  if debug < 2 then writeln('ERROR no images in a folder. See log.');
                  ptprevsource^.next := ptsource^.next;
                end;

                //log('f closed', 3, false);
              end else begin
                log('ERROR loading : ' + fullname, 0, false);
                halt;
              end;
            end;
          end;
        end;
      until FindNext(result)<>0;
      FindClose(result);
    end;
  end;
end;

function lock(lockfile: string): boolean;
var
  todo: boolean;
  f: Textfile;
  line: string;
begin
  todo := Not(FileExists(lockfile + '.run') or FileExists(lockfile + '.done'));
  if todo then begin
    try
      log('No existing lock, try to set ' + lockfile + '.run',3, true);
      assignfile(f, lockfile + '.run');
      rewrite(f);
      writeln(f, pid);
      closefile(f);
    except
      todo := False;
    end;
    sleep(3000);
    if not(FileExists(lockfile + '.run')) then todo := False;
    if todo then begin
      assignfile(f, lockfile + '.run');
      reset(f);
      readln(f, line);
      closefile(f);
      todo := (line = pid)
    end;
    if todo then
      log('Lock set.',3, true)
    else
      log('Lock did not set',3, true);
  end;
  lock := todo;
end;

function distanceham(key1, key2: TKey): integer;
begin
  distanceham := PopCnt(key1[3] xor key2[3]) + PopCnt(key1[2] xor key2[2]) + PopCnt(key1[1] xor key2[1]) + PopCnt(key1[0] xor key2[0]);
end;

procedure TMyThread.Execute;
var
  pts, ptsright: pSource;
  pti, ptiright: pFingerprint;
  sleft, sright: TSource;
  iright: TFingerprint;
  ileft: TFingerprint;
  frs: TextFile;
  nbsl, nbsr, nbil, r, threadnb, nbrs, nbcomp: integer;
  //nbir,
  lockfile, locscript, locscript2: string;
  beginleft : TDateTime;
  dur: float;

begin
  FAFinished := False;
  threadnb := i;
  locscript := script + '.' + inttostr(threadnb);
  locscript2 := '[' + locscript + '] ';
  log(locscript2 + 'Begin comparison of ' + inttostr(filecount) + ' source files.', 1, true);
  nbsl  := 0;
  if not(fileexists(locscript)) then begin
    assignfile(frs,locscript);
    rewrite(frs);
    close(frs);
  end;

  pts := firstsource;
  repeat
    beginleft := Now;
    sleft := pts^;
    inc(nbsl);
    nbsr   := 0;    // source right
    nbrs   := 0;    // resultset founds
    nbcomp := 0;
    log(locscript2 + inttostr(nbsl) + ' / ' + inttostr(filecount) + ' starting...', 3, true);
    log(locscript2 + 'SOURCE : ' + sleft.midpath + sleft.filename, 3, true);
    lockfile := folderimg + sleft.midpath + sleft.filename + '/compare.' + param;
    if lock(lockfile) then begin
      log(locscript2 + inttostr(nbsl) + ' / ' + inttostr(filecount) + ' start', 2, true);
      ptsright := firstsource;
      if not(checkall) then
         while (ptsright <> nil) and (ptsright^.filename <= sleft.filename) do begin
           log(locscript2 + '  skip ' + ptsright^.filename, 4, true);
           ptsright := ptsright^.next;
         end;
      repeat
        sright := ptsright^;
        inc(nbsr);
        nbil := 0;
        if sleft.filename <> sright.filename then begin
          pti := sleft.firstimage;
          repeat
            ileft := pti^;
            inc(nbil);
            inc(nbcomp);
            //nbir := 0;
            ptiright := sright.firstimage;

            repeat
              iright := ptiright^;
              //inc(nbir);
              //try
              r    := distanceham(ileft.key, iright.key);
              if r < threshold then begin
                inc(nbrs);
                assignfile(frs, locscript);
                append(frs);
                writeln(frs,'BEGIN. Similarity=' + inttostr(r));
                writeln(frs,'file=' + ileft.img);
                writeln(frs,'key=' + ileft.skey);
                writeln(frs,'file=' + iright.img);
                writeln(frs,'key=' + iright.skey);
                writeln(frs,'END');
                close(frs);
              end;
              //except
              //  log(locscript2 + 'ERROR : nbsl, nbsr, nbil, nbir = ' + inttostr(nbsl) + ', ' + inttostr(nbsr) + ', ' + inttostr(nbil) + ', ' + inttostr(nbir), 0, true);
              //  log(locscript2 + 'pti=' + ileft.img, 0, true);
              //  log(locscript2 + 'key=' + ileft.skey, 0, true);
              //  log(locscript2 + 'ptiright=' + ptiright^.img, 0, true);
              //  log(locscript2 + 'keyright=' + ptiright^.skey, 0, true);
              //  if ileft.next = nil then log(locscript2 + 'ileft.next = nil', 0, true);
              //  if ptiright^.next = nil then log(locscript2 + 'ptiright^.next = nil', 0, true);
              //  halt;
              //end;
              ptiright := iright.next;
            until (ptiright = nil);
            log('#' + inttostr(nbsl) + ' vs ' + inttostr(nbsr) + iif(sright.next=nil,' (last)') + '; ' + inttostr(nbil) + iif(ileft.next=nil,' (last)')
              + iif(iright.next=nil,' (last)') + ' - found ' + inttostr(nbrs), 3, true);
              //+ ' vs ' + inttostr(nbir)
            pti := ileft.next;
          until (pti = nil);
        end;
        ptsright := sright.next;
      until (ptsright = nil);

      // End of one source file. Store resultset in output file.
      RenameFile(lockfile + '.run', lockfile + '.done');
      log(locscript2 + inttostr(nbsl) + ' sources compared so far on ' + inttostr(filecount) , 2, true);
      dur := (TimeStampToMSecs(DateTimeToTimeStamp(Now)) - TimeStampToMSecs(DateTimeToTimeStamp(beginleft))) / 1000;
      log(locscript2 + 'Source ' + inttostr(nbsl) + ' / ' + inttostr(filecount) + ' done. ' + floattostr(round((nbcomp * imgcount) / (filecount * 1000000))) + 'M images compared in ' + floattostr(dur) + ' sec. Perf = ' + inttostr(round((nbcomp  * imgcount) / (filecount * dur * 1000))) + 'K comp/sec.', 1, true);

    end else begin
      log(locscript2 + 'Lock reserved by another process', 3, true);
    end;
    //log(locscript2 + 'Source done. nbsl, nbsr, nbil = ' + inttostr(nbsl) + ', ' + inttostr(nbsr) + ', ' + inttostr(nbil), 3, true);
    // + ', ' + inttostr(nbir)
    pts := sleft.next;
  until (pts = nil);

  log(locscript2 + 'THREAD FINISHED', 1, true);
  FAFinished := True;
end;

procedure comparemp;
var
  Threads: array[1..15] of TMyThread;  // Modify here to increase multi-threading limit
  ThreadCount: integer;

begin
  if nbthreads > high(Threads) then begin
    log('ERROR : Limit of ' + inttostr(high(Threads)) + '. You can modify this limit in comparemp procedure.', 0, false);
    halt;
  end;
  log('Create ' + inttostr(nbthreads) + ' threads to process comparison.',1, false);
  for i:=1 to nbthreads do begin
    Threads[i]:=TMyThread.Create(True);
    Threads[i].Start;
    sleep(10000);
  end;
  // wait till all threads finished
  repeat
    sleep(30000);
    ThreadCount := 0;
    for i:=1 to nbthreads do
      if not Threads[i].AFinished then inc(ThreadCount);
    log('Wait for ' + inttostr(ThreadCount) + ' threads to finish...', 3, false);
  until ThreadCount = 0;
  // free the threads
  for i:=nbthreads to nbthreads do
    Threads[i].Free;
  log('All comparison threads finished.', 0, false);
end;

procedure prthelp(copyright: integer);
begin
  log('************************************************************************************', 0, false);
  log('', 1, false);
  log(version, 1, false);
  log('', 1, false);
  log('Video DeDup - module compare : find video duplicates', 0, false);
  log('Copyright (C) 2018  Pierre Crette', 0, false);
  log('', 0, false);
  log('This program is free software: you can redistribute it and/or modify', copyright, false);
  log('it under the terms of the GNU General Public License as published by', copyright, false);
  log('the Free Software Foundation, either version 3 of the License, or', copyright, false);
  log('(at your option) any later version.', copyright, false);
  log('', copyright, false);
  log('This program is distributed in the hope that it will be useful,', copyright, false);
  log('but WITHOUT ANY WARRANTY; without even the implied warranty of', copyright, false);
  log('MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the', copyright, false);
  log('GNU General Public License for more details.', copyright, false);
  log('', copyright, false);
  log('You should have received a copy of the GNU General Public License', copyright, false);
  log('along with this program.  If not, see <http://www.gnu.org/licenses/>.', copyright, false);
  log('', copyright, false);
  log('SYNTAX: 2compare folderimg [options]', copyright, false);
  log('-v=n         Verbose mode. Default 1', copyright, false);
  log('-log=filenam Log file.', copyright, false);
  log('-s=file      Script to log result founds.', copyright, false);
  log('-lbl=label   Label to identify runs with different parameters. Use the same on all sessions/computers to share workload. No special characters since its use for file naming.', copyright, false);
  log('-t=n         Threshold for similarity comparison. Default 10. Performance impact.', copyright, false);
  log('-threads=n   Number of threads to use. Make tests to find better option for your computer. Performance impact.', copyright, false);
  log('-mask=n      To limit the comparison to some images files for each source file. 1/n images are used. Performance impact.', copyright, false);
  log('-masksize=n  Read n images per source then skip (mask-1)*n images', copyright, false);
  log('-maskmethod= cycle: read n images per source then skip (mask-1)*masksize images, random: if random read masksize images else skip maxsize images.', copyright, false);
  log('-checkall    Compare new sources/images against ALL other. By default only against NEXT.', copyright, false);
  log('-log=file  Log file', copyright, false);
  log('', copyright, false);
  log('************************************************************************************', 0, false);
end;

begin
  {$if declared(UseHeapTrace)}
  GlobalSkipIfNoLeaks := true; // supported as of debugger version 3.2.0
  SetHeapTraceOutput('trace.log'); // supported as of debugger version 3.2.0
  {$ifend}

  folderimg := ParamStr(1);
  if RightStr(folderimg,1) <> '/' then folderimg := folderimg + '/';
  if not(FileExists(folderimg)) then MkDir(folderimg);

  debut      := Now;
  debug      := 1;
  flogname   := '';
  copyright  := 1;
  threshold  := 10;
  mask       := 1;
  maskmethod := 'cycle';
  masksize   := 1;
  checkall   := False;
  pid        := TimeStamp;
  nbthreads  := 3;
  //testskey1  := '';
  //testskey2  := '';

  for i := 1 to ParamCount do begin
    p := ParamStr(i);
    if LeftStr(p,3)='-v='                   then debug := StrToInt(RightStr(p,length(p)-3));
    if LeftStr(p,3)='-log='                 then flogname := RightStr(p,length(p)-5);
    if LeftStr(p,3)='-s='                   then script := RightStr(p,length(p)-3);
    if LeftStr(p,5)='-lbl='                 then lbl := RightStr(p,length(p)-5);
    if LeftStr(p,3)='-t='                   then threshold := StrToInt(RightStr(p,length(p)-3));
    if LeftStr(p,3)='-nc'                   then copyright := 12;
    if LeftStr(p,6)='-mask='                then mask := StrToInt(RightStr(p,length(p)-6));
    if LeftStr(p,10)='-masksize='           then masksize := StrToInt(RightStr(p,length(p)-10));
    if LeftStr(p,12)='-maskmethod='         then maskmethod := RightStr(p,length(p)-12);
    if LeftStr(p,10)='-checkall='           then checkall := True;
    if LeftStr(p,9)='-threads='             then nbthreads := StrToInt(RightStr(p,length(p)-9));;
    //if LeftStr(p,4)='-k1='                  then testskey1 := RightStr(p,length(p)-4);
    //if LeftStr(p,4)='-k2='                  then testskey2 := RightStr(p,length(p)-4);
  end;

  prthelp(copyright);

  param := lbl + '_t_' + IntToStr(threshold) + '_' + maskmethod + '_' + IntToStr(mask) + '_' + IntToStr(masksize);
  log('param = ' + param, 1, false);

  log('debug=' + IntToStr(debug), 1, false);
  log('threshold=' + IntToStr(threshold), 1, false);
  //log('copyright=' + IntToStr(copyright), 1);
  //log('mask=' + IntToStr(mask), 1);
  //log('maskmethod=' + maskmethod, 1);
  //log('masksize=' + IntToStr(masksize), 1);
  log('script=' + script, 1, false);
  log('lbl=' + lbl, 1, false);
  log('checkall=' + BoolToStr(checkall), 1, false);
  log('threads=' + IntToStr(nbthreads), 1, false);
  //log('testskey1=' + testskey1, 1, false);
  //log('testskey2=' + testskey2, 1, false);
  log('', 0, false);

  //if testskey1 <> '' then begin
  //  testkkey1 := keytobin(testskey1);
  //  testkkey2 := keytobin(testskey2);
  //  writeln('xor : ' + inttobin(testkkey1[3] xor testkkey2[3]) + ' ' + inttobin(testkkey1[2] xor testkkey2[2]) + ' ' + inttobin(testkkey1[1] xor testkkey2[1]) + ' ' + inttobin(testkkey1[0] xor testkkey2[0]));
  //  writeln('distanceham = ' + inttostr(distanceham(testkkey1, testkkey2)));
  //  halt;
  //end;
  //if testskey2 <> '' then begin
  //  TestTimeStamp;
  //  halt;
  //end;

  filecount := 0;
  imgcount  := 0;
  firstsource := nil;
  RecurseScan(folderimg, filecount);

  if checkall then
    log(inttostr(filecount) + ' sources, ' + inttostr(imgcount) + ' images and ' + floattostr(imgcount * imgcount / 1000000) + ' millions of comparison to perform', 1, false)
  else
    log(inttostr(filecount) + ' sources, ' + inttostr(imgcount) + ' images and ' + floattostr(imgcount * imgcount / 2000000) + ' millions of comparison to perform', 1, false);

  //TestMem;
  Comparemp;

end.

