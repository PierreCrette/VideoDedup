program CompareV2;

{$mode objfpc}{$H+}
uses
{$ifdef unix}
  cthreads,
  cmem, // the c memory manager is on some systems much faster for multi-threading
  Classes,

{$endif}
  //Crt, IniFiles, Process, SysCall
  SysUtils, Math, DateUtils, crt;

{$OPTIMIZATION LEVEL3}
{$rangeChecks on}
//{$MMX on}

{
TODO :
Mask options
Other parameters missing ?
-skip to load only part of dataset and be able to compare last sources together in order to spare RAM.

v2.0 b003 : store done result in
  idxDone: array of array of boolean;
  idxSource: array of TSource;
  and replace searchs by direct acces based on index position
v2.0 b006 : store rs in memory instead of direct write on disk to improve performance when lot of result founds
v2.0.1 : testing of v2.0 b006 + minor adjustments
v2.0.2 : queue mechanism to avoid waiting for disk
v2.0.3 : -glob option (not working). Queue improvement.
v2.0.4 : log in different files the status after loading all and before computing
v2.0.5 : -glob option works and test ok (find ./db -name fingerprint.fp -exec cat {} >> ./db/glob.fp \;)
         Remove -lbl
v2.0.6 : stop.ask file to stop process neat.
}

const
  version: string = 'compare v2.0.6.2 20201110';

Type
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
    imgcount   : int64;
    next       : pSource;
    end;
  TPack = record
    count  : integer;
    imcount: integer;
    data   : array[1..10000] of integer;
    end;
  Tqueuelt = record
    qleft    : integer;
    qpack    : TPack;
    qcomment : string;
    qadvanct : double;
  end;

  TMyThread = class(TThread)
  private
    FAFinished:  boolean;
    FALeft:      integer;
    FARight:     TPack;
    FAthreadnb:  integer;
    FAcomment:   string;
    FAadvanct:   double;
  public
    procedure Execute; override;
    property AFinished: boolean read FAFinished write FAFinished;
    property ALeft:     integer read FALeft write FALeft;
    property ARight:    TPack read FARight write FARight;
    property Athreadnb: integer read FAthreadnb write FAthreadnb;
    property Acomment:  string read FAcomment write FAcomment;
    property Aadvanct:  double read FAadvanct write FAadvanct;
  end;

var
  Threads: array[1..64] of TMyThread;  // Modify here to increase multi-threading limit
  Threadstatus: array[1..64] of integer; // and here. 0=free, 1= running, 2=writing
  queue: array[0..128] of Tqueuelt;
  queuelen, queuemaxlen, queuemin, queuemax: integer;

  idxDone: array of array of boolean;
  idxSource: array of TSource;

  folderimg, flogname, script: string;
  debug, threshold, copyright, cptdisplay : integer;
  filecount, imgcount, nbthreads, errimg, nbcrossing: integer;
  p, pid, fdbexclude: string;
  firstsource: pSource;
  debut, debutcompute, LastLoading, t: TDateTime;
  previouscomputetime: Qword;
  clean, glob, upgradedone, firstload, stopaskedflag: boolean;

  //testskey1, testskey2: string;
  //testkkey1, testkkey2: TKey;

procedure CreateFile(filename: string);
var
  f: file;
begin
  if not(fileexists(filename)) then begin
    assignfile(f,filename);
    rewrite(f);
    close(f);
  end;
end;

function Unites(n: float): string;
var
  tmp: float;
  u: string;
begin
  if n > 11000000000000 then begin
    tmp := n / 1000000000000;
    u := ' T';
  end else begin
    if n > 11000000000 then begin
      tmp := n / 1000000000;
      u := ' G';
    end else begin
      if n > 11000000 then begin
        tmp := n / 1000000;
        u := ' M';
      end else begin
        if n > 11000 then begin
          tmp := n / 1000;
          u := ' K';
        end else begin
          tmp := n;
          u := '';
        end;
      end;
    end;
  end;
  Unites := formatfloat('0', tmp) + u;
  //'# ##0.000'
end;

function msToStr(durms: Qword; days:boolean): string;
var
  dur, sec, mn: Qword;
  tmp     : string;
begin
  dur := durms div 1000;
  sec := dur mod 60;
  dur := (dur - sec) div 60;
  mn  := dur mod 60;
  dur := (dur - mn) div 60;

  if days then begin
    if dur<10 then
      tmp := '0' + inttostr(dur) + ':' + rightstr('0'+inttostr(mn),2)
    else
      tmp := inttostr(dur) + ':' + rightstr('0'+inttostr(mn),2);
  end else begin
    if durms < 60000 then
      tmp := inttostr(sec) + '"' + rightstr('00'+inttostr(durms mod 1000),3)
    else
      if durms < 3600000 then
        tmp := inttostr(mn) + '''' + rightstr('0'+inttostr(sec),2) + '"'
      else
        if dur < 24 then
          tmp := rightstr('0'+inttostr(dur),2) + 'h' + rightstr('0'+inttostr(mn),2)
        else
          tmp := inttostr(dur) + ' hours';
  end;
  msToStr := tmp;
end;

function Duration(tdebut, tfin: TDateTime): Comp;
begin
  Duration := TimeStampToMSecs(DateTimeToTimeStamp(tfin)) - TimeStampToMSecs(DateTimeToTimeStamp(tdebut));
end;

function DurationToStr(tdebut, tfin: TDateTime; kind: integer): string;
var
  dur     : Comp;
  durts   : TTimeStamp;
  tmp     : string;
begin
  dur   := Duration(tdebut, tfin);
  if kind = 0 then begin
    tmp := msToStr(round(dur), true);
  end else begin
    durts := MSecsToTimeStamp(dur);
    if (durts.Date > 0) or (kind = 0) then begin
      tmp := inttostr(durts.Date) + 'j' + msToStr(durts.Time, true);
    end else begin
      tmp := msToStr(durts.Time, false);
    end;
  end;
  DurationToStr := tmp;
end;

function TimeStamp(kind: integer): string;
var
  st      : TSystemTime;
  current : TDateTime;
begin
  current := Now;
  DateTimeToSystemTime(current,st);
  if kind = 0 then begin// Standard display
    TimeStamp := inttostr(st.year) + '/' + rightstr('0'+inttostr(st.month),2) + '/' + rightstr('0'+inttostr(st.Day),2)
    + ' ' + rightstr('0'+inttostr(st.Hour),2) + ':' + rightstr('0'+inttostr(st.minute),2) + '''' + rightstr('0'+inttostr(st.Second),2) + ' ';
    if debug > 0 then
      TimeStamp := TimeStamp + '(' + DurationToStr(debut, current, 0) + ') ';
  end;
  if kind = 1 then // pid
    TimeStamp := inttostr(st.year) + rightstr('0'+inttostr(st.month),2) + rightstr('0'+inttostr(st.Day),2) + '.'
    + rightstr('0'+inttostr(st.Hour),2) + rightstr('0'+inttostr(st.minute),2) + rightstr('0'+inttostr(st.Second),2) + inttostr(st.MilliSecond);
end;

procedure Log(s: string; verbose: integer; fname: string=''); //inthread: boolean
var
  flog: TextFile;
  fnameloc, output: string;
begin
  if verbose-debug < 3 then begin
    //loglevel enough for file logging
    output := TimeStamp(0) + s;
    if (fname = '') or (fname='mp') then fnameloc := flogname else fnameloc := fname;
    if fnameloc <> '' then begin
      if FileExists(fnameloc) then begin
        assignfile(flog, fnameloc);
        append(flog);
        writeln(flog, output);
        close(flog);
      end else begin
        assignfile(flog, fnameloc);
        rewrite(flog);
        writeln(flog, output);
        close(flog);
      end;
    end;
  end;
  if verbose <= debug then begin
    //loglevel enough for screen
    if fname='mp' then begin
      //tmporary storage in a file for MP due to unavailability to do it properly from threads
      fnameloc := folderimg + 'mp.log';
      if FileExists(fnameloc) then begin
        assignfile(flog, fnameloc);
        append(flog);
        writeln(flog, output);
        close(flog);
      end else begin
        assignfile(flog, fnameloc);
        rewrite(flog);
        writeln(flog, output);
        close(flog);
      end;
    end else begin
      writeln(output);
    end;
  end;
end;

procedure LogCompute(s: string; avt: double); //inthread: boolean
var
  flc: TextFile;
  fnameloc, output, msg: string;
  durms: Comp;
  durh: double;
begin
  durms := previouscomputetime + TimeStampToMSecs(DateTimeToTimeStamp(Now)) - TimeStampToMSecs(DateTimeToTimeStamp(debutcompute));
  durh := durms * 0.000000278;
  output := floattostr(durms) + ' ; ' + s;
  msg := formatfloat('0.0', durh) + 'h done, ' + formatfloat('0.00',durh / avt) + 'h est. and ETA=' + formatfloat('0.0',durh / avt - durh) + 'h - ' + s;
  log(msg, 0, 'mp');
  fnameloc := folderimg + 'compute.log';
  if FileExists(fnameloc) then begin
    assignfile(flc, fnameloc);
    append(flc);
    writeln(flc, output);
    close(flc);
  end else begin
    assignfile(flc, fnameloc);
    rewrite(flc);
    writeln(flc, output);
    close(flc);
  end;
end;

function StopAsked: boolean;
var
  flog: TextFile;
  s: string;
begin
  //Display MP log
  if FileExists(folderimg + 'mp.log') then begin
    assignfile(flog, folderimg + 'mp.log');
    reset(flog);
    //writeln('BEGIN');
    while not eof(flog) do begin
      readln(flog, s);
      writeln(s);
    end;
    //writeln('END');
    close(flog);
    deletefile(folderimg + 'mp.log');
  end;
  //Test is stop is asked by user
  if stopaskedflag then begin
    log('Stopasked variable set. Do not check again.', 3);
  end else begin
    if keypressed then begin
      s:=inttostr(ord(ReadKey));
      log('Keypressed #' + s, 3);
      if (s='27') or (s='3') then begin {Esc}
        stopaskedflag := True;
        log('ESC KEY PRESSED TO ASK STOP ...', 0);
      end;
    end;
    if FileExists(folderimg + 'stop.ask') then begin
      stopaskedflag := True;
      log('STOP ASKED BY FILE SET (stop.ask) ...', 0);
    end;
  end;
  stopasked := stopaskedflag;
end;

procedure LoadPreviousComputetime;
var
  flc: TextFile;
  fnameloc, lgn: string;
  ms: Qword;
  i: integer;
begin
  previouscomputetime := 0;
  fnameloc := folderimg + 'compute.log';
  if FileExists(fnameloc) then begin
    assign(flc, fnameloc);
    reset(flc);
    while not eof(flc) do begin
      readln(flc, lgn);
      try
        i:=1;
        while lgn[i] <> ';' do inc(i);
        ms := strtoint(lgn[1..i-2]);
        if ms > previouscomputetime then
          previouscomputetime := ms;
      except
        log('ERROR reading duration of line ' + lgn, 0);
      end;
    end;
    close(flc);
  end;
  debutcompute := Now;
  log('Previous compute time = ' + floattostr(previouscomputetime/3600000) + ' hours', 0);
end;

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

    if cpt <> 0 then begin
      dec(cpt);
    end else begin
      if deb <> len then begin
        log('ERROR keytobin ' + key, 0);
        log('deb = ' + inttostr(deb), 0);
        log('len = ' + inttostr(len), 0);
        log('k[len] = ' + inttostr(k[len]), 0);
      end;
    end;
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

function FindPrevSource(mname: string): pSource;
var
  pt, prev: pSource;
begin
  //return nil if list empty or 1st
  //could be optimized by dichotomy
  pt   := firstsource;
  prev := nil;
  while (pt <> nil) and (mname > pt^.filename) do begin
    prev := pt;
    pt   := pt^.next;
  end;
  FindPrevSource := prev;
end;

procedure InitIdx;
var
  i, j: integer;
  ps: pSource;
  t: TDateTime;
begin
  t := Now;
  setLength(idxSource, filecount);
  setLength(idxDone, filecount, filecount);
  //Clean if duplicate filename
  ps   := firstsource;
  while ps <> nil do begin
    if (ps^.next <> nil) and (ps^.filename = ps^.next^.filename) then begin
      log('InitIdx: ERROR 2 files with same name. One is discarded. Relaunch 1f_Parse.py -clean to solve. ' + ps^.filename + ' and ', 0);
      log(ps^.next^.filename, 0);
      ps^.next := ps^.next^.next;
      dec(filecount);
      inc(errimg);
    end;
    ps := ps^.next
  end;

  //Populate idxDone as a square matrix of done/todo video pairs
  ps := firstsource;
  for i:=0 to filecount-1 do begin
    if ps <> nil then begin
      idxSource[i] := ps^;

      log('InitIdx[' + inttostr(i) + '] = ' + idxSource[i].filename, 2, 'InitIdx.txt');

      for j:=0 to filecount-1 do
        idxDone[i][j] := false;
      ps := ps^.next;
    end else begin
      log('ERROR InitIdx ps = nil', 1);
    end;
  end;
  log('InitIdx done in ' + DurationToStr(t, Now, 1), 1);
end;

procedure grabstats;
var
   i,j: integer;
begin
  nbcrossing := 0;
  for i:=0 to filecount-2 do
    for j:=i+1 to filecount-1 do
      if idxDone[i][j] then inc(nbcrossing);
end;

function SeekIdxSource(InFilename: string): integer;
var
  m, n, min, max, IndexSource: integer;
begin
  IndexSource := -1;
  min := 0;
  max := filecount;
  n := 0;
  repeat
    m := n;
    n := (min + max) div 2;

    if idxSource[n].filename > InFilename then max := n
    else min := n;

    if idxSource[n].filename = InFilename then IndexSource := n
    else if (n > 0) and (idxSource[n-1].filename = InFilename) then IndexSource := n-1
    else if (n < filecount) and (idxSource[n+1].filename = InFilename) then IndexSource := n+1
    else if abs(m - n) < 2 then begin
      IndexSource := 999999999;
      log('SeekIdxSource: Not found in Index (n=' + inttostr(n) + ') "' + InFilename + '"', 3);
    end;

  until IndexSource > -1;
  SeekIdxSource := IndexSource;
end;

procedure GlobScan(fold: string; var filecount:integer);
var
  fullname, imgpath, previmgpath, skey, simg: string;
  f: TextFile;
  pt, prev: pFingerprint;
  ptsource, ptprevsource: pSource;
  imgcountloc: integer;

begin
  fullname := fold + 'glob.fp';
  if fileexists(fullname) then begin
    assignfile(f, fullname);
    reset(f);
    ptsource := nil;
    previmgpath := '';
    imgcount := 0;
    imgcountloc:=0;
    while not eof(f) do begin
      readln(f, skey);
      if leftstr(skey,4) <> 'key=' then begin
         Log('Error : in ' + fullname + ' Look for key= and found ' + skey, 0);
         halt;
      end;
      readln(f, simg);
      if leftstr(simg,5) <> 'file=' then begin
         Log('Error : in ' + fullname + ' Look for file= and found ' + simg, 0);
         halt;
      end;
      imgpath := simg[6..length(simg)-13];
      if previmgpath <> imgpath then begin
        previmgpath := imgpath;
        inc(filecount);
        if ptsource <> nil then begin
          ptsource^.imgcount := imgcountloc;
          imgcountloc := 0;
        end;
        new(ptsource);
        ptsource^.midpath  := '';
        ptsource^.filename := EndName(simg);
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
      end;

      inc(imgcount);
      inc(imgcountloc);
      new(pt);
      pt^.skey   := rightstr(skey, length(skey)-4);
      pt^.key    := keytobin(rightstr(skey, length(skey)-4));
      pt^.img    := rightstr(simg, length(simg)-5);
      pt^.next   := nil;
      if prev = nil then
        ptsource^.firstimage := pt
      else
        prev^.next := pt;
      prev       := pt;
    end;

    closefile(f);
    if prev = nil then begin
      log('ERROR no images in ' + fullname, 3);
      if debug < 3 then log('ERROR no images in a folder. See log.', 0);
      ptprevsource^.next := ptsource^.next;
      dec(filecount);
    end;
    ptsource^.imgcount := imgcountloc;

  end else begin
    log('ERROR loading : ' + fold + 'glob.fp', 0);
    halt;
  end;
end;

procedure RecurseScan(fold: string; var filecount: integer);
// Read *.fp in folder and store their content in memory : sources and image/key pairs
var
  fullname, skey, simg: string;
  result: TRawByteSearchRec;
  f: TextFile;
  pt, prev: pFingerprint;
  ptsource, ptprevsource: pSource;
  imagecount: integer;

  procedure OneFP();
  begin
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

      imagecount := 0;
      prev := nil;
      while not eof(f) do begin
        readln(f, skey);
        if leftstr(skey,4) <> 'key=' then begin
           Log('Error : in ' + fullname + ' Look for key= and found ' + skey, 0);
           halt;
        end;
        readln(f, simg);
        if leftstr(simg,5) <> 'file=' then begin
           Log('Error : in ' + fullname + ' Look for file= and found ' + simg, 0);
           halt;
        end;

        inc(imagecount);
        inc(imgcount);
        new(pt);
        pt^.skey   := rightstr(skey, length(skey)-4);
        pt^.key    := keytobin(rightstr(skey, length(skey)-4));
        pt^.img    := ImageName(simg, fold);

        //log('simg='+ImageName(simg, fold),0);

        pt^.next   := nil;
        if prev = nil then
          ptsource^.firstimage := pt
        else
          prev^.next := pt;
        prev       := pt;
      end;
      closefile(f);

      if prev = nil then begin
        log('ERROR no images in ' + fullname, 3);
        if debug < 3 then log('ERROR no images in a folder. See log.', 0);
        ptprevsource^.next := ptsource^.next;
        dec(filecount);
      end;
      ptsource^.imgcount := imagecount;

    end else begin
      log('ERROR loading : ' + fullname, 0);
      halt;
    end;
  end;

begin
  fold := ExpandFileName(fold);
  if fold[length(fold)]='/' then fold := leftstr(fold,length(fold)-1);
  if fold <> folderimg + 'unwanted' then begin
    if (filecount = cptdisplay) then begin
      if cptdisplay > 0 then
        log('Loaded ' + inttostr(filecount) + ' images folders...', 1);
      cptdisplay := cptdisplay + 1000;
    end;
    if FindFirst(fold + '/*', faAnyFile and faDirectory, result)=0 then begin
      repeat
        if (result.Name <> '.') and (result.Name <> '..') then begin
          If (result.Attr and faDirectory) = faDirectory then begin
            recursescan(fold + '/' + result.Name, filecount);
          end else begin
            fullname := fold + '/' + result.Name;
            if clean and (RightStr(result.Name, 4) = '.run') then begin
              log('DeleteFile ' + fullname, 2);
              DeleteFile(fullname);
            end;
            if clean and (RightStr(result.Name, 5) = '.done') then begin
              log('DeleteFile ' + fullname, 2);
              DeleteFile(fullname);
            end;
            if upcase(RightStr(result.Name, 3)) = '.FP' then begin
              if (LeftStr(result.Name, 4) <> 'glob') and not glob then begin
                inc(filecount);
                fullname := fold + '/' + result.Name;
                OneFP;
              end;
            end;
          end;
        end;
      until FindNext(result)<>0;
      FindClose(result);
    end;
  end;
end;

function distanceham(key1, key2: TKey): integer;
begin
  distanceham := PopCnt(key1[3] xor key2[3]) + PopCnt(key1[2] xor key2[2]) + PopCnt(key1[1] xor key2[1]) + PopCnt(key1[0] xor key2[0]);
end;

function lock(lockfile, pid, kind: string): boolean;
//kind=fast: set lock without control, =ctrl:after a fast perform the control, =full: fast+ctrl
var
  todo: boolean;
  f: Textfile;
  line: string;
  t: TDateTime;
begin
  t := Now;
  log('Lock ' + lockfile + ', ' + kind, 3);
  if (kind<>'full') and (kind<>'fast') and (kind<>'ctrl') and (kind<>'delete') then begin
    log('ERROR kind of lock : ' + kind,1);
    halt;
  end;
  if kind='delete' then begin
    log('Try to delete ' + lockfile, 3);
    assignfile(f, lockfile);
    reset(f);
    readln(f, line);
    closefile(f);
    if (line = pid) then begin
      try
        deletefile(lockfile);
      except
        log('Lock delete of ' + lockfile + ' error. Skiped.', 3);
      end;
    end;
  end else begin
    todo := Not(FileExists(lockfile + '.run'));
    if todo or (kind='ctrl') then begin
      if (kind='ctrl') then begin
        todo := True;
      end else begin
        try
          log('No existing lock, try to set ' + lockfile + '.run',3);
          assignfile(f, lockfile + '.run');
          rewrite(f);
          writeln(f, pid);
          closefile(f);
        except
          todo := False;
        end;
      end;
      if (kind = 'ctrl') or (kind='full') then begin
        if (kind='full') then sleep(3000); //else sleep(1000);
        if not(FileExists(lockfile + '.run')) then todo := False;
        if todo then begin
          assignfile(f, lockfile + '.run');
          reset(f);
          readln(f, line);
          closefile(f);
          todo := (line = pid)
        end;
        if todo then
          log('Lock set.',3)
        else
          log('Lock did not set',3);
      end;
    end;
    lock := todo;
  end;
  log('Lock in ' + DurationToStr(t, Now, 1), 3);
end;

procedure LoadDone(position: integer; clean: boolean);
var
  f: Textfile;
  result: TRawByteSearchRec;
  line,lstr,rstr, s: string;
  i, j, nbfiles: integer;
  nb, delim, notfound: integer;
  newfile: boolean;
  t: TDateTime;
begin
  t := Now;
  if firstload then log('Firstload of previously done comparison...', 1);
  nb := 0;
  notfound := 0;
  nbfiles := 0;
  if FindFirst(folderimg + '*.db', faAnyFile and faDirectory, result)=0 then begin
    repeat
      //log('Found : ' + result.Name, 2);
      newfile := (CompareDateTime(FileDateToDateTime(FileAge(folderimg + result.Name)), LastLoading) > 0);
      if (result.Name <> '.') and (result.Name <> '..') and (result.Name <> fdbexclude) and ((result.Attr and faDirectory) <> faDirectory) and (newfile or firstload) then begin
        assignfile(f,folderimg + result.Name);
        reset(f);
        inc(nbfiles);
        while not(eof(f)) do begin
          readln(f, line);
          delim := pos(';',line);
          lstr := leftstr(line, delim - 2);
          rstr := rightstr(line, length(line) - delim - 1);

          if (length(lstr) > 0) and (length(rstr) > 0) then begin
            // DEBUG: test useless except if bug in .db generation
            i := SeekIdxSource(lstr);
            j := SeekIdxSource(rstr);
          end else begin
            i := 999999999;
            j := 999999999;
          end;

          if (i <> 999999999) and (j <> 999999999) then begin
            inc(nb);
            idxDone[i][j] := True;

          end else begin
            inc(notfound);
            if i=999999999 then
              log('-- Not found but in ' + result.Name + ' : ' + lstr, 6)
            else
              log('-- Not found but in ' + result.Name + ' : ' + rstr, 6)
          end;

          if ((nb + notfound) mod 10000000 = 0) then
            log('  read line ' + unites(nb + notfound) + ' from ' + result.Name, 1);

        end;
        close(f);
      end;
      if firstload then
        log('  read line ' + unites(nb + notfound) + ' from ' + result.Name, 2);
    until FindNext(result)<>0;
    FindClose(result);
    if firstload then begin
      log('************************************************************************************************************************', 1);
      log(unites(nb) + ' source pairs done / ' + unites(0.5 * filecount * filecount) + ' possible pairs = ' + formatfloat('0.00',200 * nb / filecount / filecount) + '% RAW', 1);
      log('************************************************************************************************************************', 1);
      nbcrossing := nb;
    end else log(unites(nb) + ' source pairs done / ' + unites(0.5 * filecount * filecount) + ' possible pairs = ' + formatfloat('0.00',200 * nb / filecount / filecount) + '% RAW', 2);
  end;

  if clean then begin
    // Write new DB without inconsistencies
    fdbexclude := 'compdone' + TimeStamp(1) + '.db';
    CreateFile(folderimg + fdbexclude);
    assignfile(f, folderimg + fdbexclude);
    append(f);
    for i:=0 to filecount-2 do
      for j:=i+1 to filecount-1 do
        if idxDone[i][j] or idxDone[j][i] then
          writeln(f, idxSource[i].filename + ' ; ' + idxSource[j].filename);
    close(f);
    log('Created ' + fdbexclude, 1);
    // Delete old DB files
    if FindFirst(folderimg + '*.db', faAnyFile and faDirectory, result)=0 then begin
      repeat
        if (result.Name <> '.') and (result.Name <> '..') and ((result.Attr and faDirectory) <> faDirectory) and (result.Name <> fdbexclude) then
          try
            log('Removing ' + result.Name, 2);
            if not DeleteFile(folderimg + result.Name) then begin
              log('Remove failed. Please remove *.db manually except last one.', 1);
              log('rm ' + result.Name, 1);
            end;
          except
            log('Remove failed. Please remove *.db manually except last one.', 1);
            log('rm ' + result.Name, 1);
          end
        else
          log('Not removing ' + result.Name, 2);
      until FindNext(result)<>0;
      FindClose(result);
    end;
  end;

  LastLoading := t;
  //if (nb > 0) or (position mod 100 = 0) then i:=1 else i:=2;
  //Must be reload for each source to get other computers work.
  s := inttostr(position) + ' / ' + inttostr(filecount) + ' sources, ' + unites(nbcrossing) + ' / ' + unites(filecount * filecount * 0.5) + ' source pairs, ';
  s := s + inttostr(nb) + ' pairs read in *.db in ' + DurationToStr(t, Now, 1);
  s := s + '. Queuesize = ' + inttostr(queuelen) + ' (min ' + inttostr(queuemin) + '). ';
  if notfound > 0 then s := s + inttostr(notfound) + ' NOT FOUND ';
  log(s, 1);
  firstload := false;
end;

procedure Closure;
var
  result: TRawByteSearchRec;
begin
  log('Cleanning lock files (*.run)', 0);
  if FindFirst(folderimg + '*.run', faAnyFile and faDirectory, result)=0 then begin
    repeat
      lock(folderimg + result.Name, pid, 'delete');
    until FindNext(result)<>0;
    FindClose(result);
  end;
  log('Compressing *.db into 1 file.', 0);
  loaddone(0, True);
  if FileExists(folderimg + 'stop.ask') then DeleteFile(folderimg + 'stop.ask')
  else log(folderimg + 'stop.ask is not present', 0);
  if FileExists(folderimg + 'mp.log') then DeleteFile(folderimg + 'mp.log')
  else log(folderimg + 'mp.log is not present', 0);
  log('FINISHED.', 0);
end;

procedure TMyThread.Execute;
var
  pti, ptiright: pFingerprint;
  iright, ileft: TFingerprint;
  f, frs: TextFile;
  r, cpt: integer;
  locscript, locscript2, line, txtrs: string;
  beginleft, endleft : TDateTime;
  durms: Comp;

begin
  beginleft := Now;
  FAFinished := False;
  locscript  := folderimg + script + '.' + inttostr(FAthreadnb);
  if nbthreads > 99 then
    locscript2 := '[ ' + formatfloat('000', FAthreadnb) + ' ] '
  else if nbthreads > 9 then
    locscript2 := '[ ' + formatfloat('00', FAthreadnb) + ' ] '
  else begin
    locscript2 := '[';
    for r :=1 to nbthreads do
      if r = FAthreadnb then
        locscript2 := locscript2 + inttostr(FAthreadnb)
      else
        locscript2 := locscript2 + ' ';
    locscript2 := locscript2 + '] ';
  end;
  CreateFile(locscript + '.rs');
  log(locscript2 + 'Begin Thread #' + inttostr(FAthreadnb) + ' ' + FAcomment + ' with count = ' + inttostr(FARight.count) + ', imcount = ' + inttostr(FARight.imcount)
  + ', for ' + inttostr(idxSource[FALeft].imgcount) + ' left images.', debug+1);

  //open & close file cannot be outside the loop because killing the program would let results not written to disk
  txtrs := '';
  for cpt := 1 to FARight.count do begin
    pti    := idxSource[FAleft].firstimage;
    repeat
      ileft := pti^;
      ptiright := idxSource[FAright.data[cpt]].firstimage;
      repeat
        iright := ptiright^;
        r    := distanceham(ileft.key, iright.key);
        if r < threshold then begin
          txtrs   := txtrs + 'BEGIN. Similarity=' + inttostr(r) + LineEnding;
          txtrs   := txtrs + 'file=' + ileft.img + LineEnding;
          txtrs   := txtrs + 'key=' + ileft.skey + LineEnding;
          txtrs   := txtrs + 'file=' + iright.img + LineEnding;
          txtrs   := txtrs + 'key=' + iright.skey + LineEnding;
          txtrs   := txtrs + 'END' + LineEnding;
        end;
        ptiright := iright.next;
      until (ptiright = nil);
      pti := ileft.next;
    until (pti = nil);
  end;
  assignfile(frs, locscript + '.rs');
  append(frs);
  write(frs,txtrs);
  close(frs);

  CreateFile(folderimg + 'compdone.' + pid + '.' + inttostr(FAthreadnb) + '.' + inttostr(FAleft + 1) + '.db');
  assignfile(f, folderimg + 'compdone.' + pid + '.' + inttostr(FAthreadnb) + '.' + inttostr(FAleft + 1) + '.db');
  append(f);
  for cpt := 1 to FARight.count do begin
    if (length(idxSource[FAleft].filename) > 1) and (length(idxSource[FAright.data[cpt]].filename) > 1) then begin
      if idxSource[FAleft].filename < idxSource[FAright.data[cpt]].filename then
        line := idxSource[FAleft].filename + ' ; ' + idxSource[FAright.data[cpt]].filename
      else
        line := idxSource[FAright.data[cpt]].filename + ' ; ' + idxSource[FAleft].filename;
      writeln(f, line);
    end;
  end;
  close(f);

  endleft := Now;
  durms := duration(beginleft, endleft);
  txtrs := locscript2 + FAcomment + ' in ' + DurationToStr(beginleft, endleft, 1) + ', '
    + unites(FARight.count) + ' sources for ' + unites(FARight.imcount * idxSource[FAleft].imgcount) + ' comp @ '
    + unites(1000 * FARight.imcount * idxSource[FAleft].imgcount / durms) + ' c/s';
  LogCompute(txtrs, FAadvanct);
  Threadstatus[FAthreadnb] := 0;
  FAFinished := True;
end;

procedure LoopSources;
var
  sleft, sright: integer;
  spack: TPack;
  i, nbpack: integer;
  lockfile, prevlockfile, lockctrl, durload: string;
  ThreadLaunched, ThreadQueued : Boolean;
  t1, t2: TDateTime;

  procedure ThreadWait;
  var
    i, nbactive: integer;
  begin
    repeat
      sleep(500);
      nbactive := 0;
      for i:=1 to nbthreads do
        if Threadstatus[i] > 0 then
          inc(nbactive)
        else
          if queuelen > 0 then begin
            Threadstatus[i] := 1;
            Threads[i] := TMyThread.Create(True);
            Threads[i].FreeOnTerminate := True;
            Threads[i].Athreadnb       := i;
            if queuelen > queuemax then queuemax := queuelen;
            if (queuelen < queuemin) and (queuemax = queuemaxlen) then queuemin := queuelen;
            //if queuemin = 0 then queuemin := queuemaxlen;
            dec(queuelen);
            inc(nbactive);
            Threads[i].ALeft    := queue[queuelen].qleft;
            Threads[i].ARight   := queue[queuelen].qpack;
            Threads[i].Acomment := queue[queuelen].qcomment;
            Threads[i].Aadvanct := queue[queuelen].qadvanct;
            Threads[i].Start;
          end;
    until (nbactive=0) or stopasked;
  end;

  procedure ThreadExec(comment: string; advanct: double);
  var
    i: integer;
    qelt: Tqueuelt;
  begin
    ThreadLaunched := False;
    ThreadQueued   := False;
    qelt.qleft     := sleft;
    qelt.qpack     := spack;
    qelt.qcomment  := comment;
    qelt.qadvanct  := advanct;
    repeat
      for i:=1 to nbthreads do begin
        if not(ThreadLaunched) and (Threadstatus[i] = 0) then begin
          Threadstatus[i] := 1;
          Threads[i] := TMyThread.Create(True);
          Threads[i].FreeOnTerminate := True;
          Threads[i].Athreadnb       := i;
          if queuelen > 0 then begin
            if queuelen > queuemax then queuemax := queuelen;
            if queuelen < queuemin then queuemin := queuelen;
            //if queuemin = 0 then queuemin := 2 * nbthreads;
            dec(queuelen);
            Threads[i].ALeft    := queue[queuelen].qleft;
            Threads[i].ARight   := queue[queuelen].qpack;
            Threads[i].Acomment := queue[queuelen].qcomment;
            Threads[i].Aadvanct := queue[queuelen].qadvanct;
          end else begin
            Threads[i].ALeft    := qelt.qleft;
            Threads[i].ARight   := qelt.qpack;
            Threads[i].Acomment := qelt.qcomment;
            Threads[i].Aadvanct := qelt.qadvanct;
            ThreadLaunched := True;
          end;
          Threads[i].Start;
        end;
      end;
      if not(ThreadLaunched) then begin
        if queuelen < queuemaxlen then begin
          queue[queuelen] := qelt;
          ThreadQueued    := True;
          inc(queuelen);
        end;
      end;
      if not(ThreadLaunched or ThreadQueued) then begin
        log('Queue full. Waiting.', 6);
        grabstats;
        sleep(2000);
      end;
    until ThreadLaunched or ThreadQueued or stopasked;
  end;

begin
  if nbthreads > high(Threads) then begin
    log('ERROR : Number of threads limited to ' + inttostr(high(Threads)) + '. You can modify this limit in LoopSources procedure.', 0);
    halt;
  end;
  for i := 1 to nbthreads do Threadstatus[i] := 0;
  prevlockfile   := '';
  queuelen     := 0;
  queuemaxlen  := 2 * nbthreads;
  queuemin     := queuemaxlen;
  queuemax     := 0;

  for sleft := 0 to filecount-1 do begin
    t1 := Now;
    lockfile := folderimg + idxSource[sleft].midpath + idxSource[sleft].filename + '.compare';
    if not(stopasked) then begin
      if lock(lockfile, pid, 'fast') then begin
        lockctrl := 'not tested';
        nbpack := 0;
        spack.count   := 0;
        spack.imcount := 0;
        t2 := Now;
        loaddone(sleft, False);
        durload := DurationToStr(t2, Now, 1);

        for sright := 0 to filecount-1 do begin
          // Loop all right not only when sright>sleft. Then double check in idxDone
          if not(idxDone[sleft][sright]) and not(idxDone[sright][sleft]) and (sleft<>sright) then begin
            if lockctrl = 'not tested' then
              if lock(lockfile, pid, 'ctrl') then lockctrl := 'ok' else lockctrl := 'exit';
            if lockctrl = 'ok' then begin
              log('START COMP ' + inttostr(sleft) + ' / ' + inttostr(filecount) + ', ' + inttostr(sleft) + ' vs ' + inttostr(sright) + ' : ' + idxSource[sleft].filename + ' ; ' + idxSource[sright].filename, 4);
              inc(spack.count);
              spack.imcount := spack.imcount + idxSource[sright].imgcount;
              spack.data[spack.count] := sright;
              if (spack.imcount * idxSource[sleft].imgcount > 10000000000) or (spack.count = high(spack.data)) then begin
                inc(nbpack);
                ThreadExec('Source ' + inttostr(sleft) + '/' + inttostr(filecount) + ', pack ' + inttostr(nbpack) + ' (' + formatfloat('0.00',100 * (1 - sqr(filecount-sleft) / sqr(filecount))) + '%)', 1 - sqr(filecount-sleft) / sqr(filecount));
                spack.count   := 0;
                spack.imcount := 0;
              end;
            end;
          end;
        end;
        if (spack.count > 0) and (lockctrl = 'ok') then begin
          inc(nbpack);
          ThreadExec('Source ' + inttostr(sleft) + '/' + inttostr(filecount) + ', last pack ' + inttostr(nbpack) + ' (' + formatfloat('0.00',100 * (1 - sqr(filecount-sleft) / sqr(filecount))) + '%)', 1 - sqr(filecount-sleft) / sqr(filecount));
        end;
      end else begin
        log('Lock reserved by another process', 3);
      end;
    end;

    if (lockctrl = 'ok') and FileExists(prevlockfile + '.run') then begin
      try
        log('Finished. Removing ' + prevlockfile + '.run', 3);
        DeleteFile(prevlockfile + '.run');
      except
        log('Cannot remove ' + prevlockfile + '.run', 0);
      end;
      prevlockfile := lockfile;
      log('Duration to prepare source #' + inttostr(sleft) + ' is ' + DurationToStr(t1, Now, 1) + ', including ' + durload + ' for loading updated .db (+ wait if queue is full)', 2);

    end;
    log('1 left source in ' + DurationToStr(t1, Now, 1), 3);
  end;
  // Main program finished: no more data to process
  ThreadWait;
  if (lockctrl = 'ok') and FileExists(prevlockfile + '.run') then begin
    try
      //Why removing previous lock and not current? Is there a delay to respect due to MP?
      log('Finished. Removing ' + prevlockfile + '.run', 2);
      DeleteFile(prevlockfile + '.run');
    except
      log('Cannot remove ' + prevlockfile + '.run', 1);
    end;
  end;
end;

procedure PrtHelp(copyright: integer);
begin
  log('************************************************************************************', 0);
  log('', 1);
  log(version, 1);
  log('', 1);
  log('Video DeDup - module compare : find video duplicates', 0);
  log('Copyright (C) 2018, 2019  Pierre Crette', 0);
  log('Version ' + version, 0);
  log('', 0);
  log('This program is free software: you can redistribute it and/or modify', copyright);
  log('it under the terms of the GNU General Public License as published by', copyright);
  log('the Free Software Foundation, either version 3 of the License, or', copyright);
  log('(at your option) any later version.', copyright);
  log('', copyright);
  log('This program is distributed in the hope that it will be useful,', copyright);
  log('but WITHOUT ANY WARRANTY; without even the implied warranty of', copyright);
  log('MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the', copyright);
  log('GNU General Public License for more details.', copyright);
  log('', copyright);
  log('You should have received a copy of the GNU General Public License', copyright);
  log('along with this program.  If not, see <http://www.gnu.org/licenses/>.', copyright);
  log('', copyright);
  log('SYNTAX: 2compare folderimg [options]', copyright);
  log('-v=n         Verbose mode. Default 1', copyright);
  log('-s=file      Script to log result founds.', copyright);
  log('-t=n         Threshold for similarity comparison. Default 10. Performance impact.', copyright);
  log('-threads=n   Number of threads to use. Make tests to find better option for your computer. Performance impact.', copyright);
  log('-clean       Read all DB files, remove references to old files, remove duplicates, store all in 1 file.', copyright);
  log('-glob        Read db/glob.fp instead of parsing all folders for fingerprint.fp. Faster but user must ensure glob.fp updates.', copyright);
  log('-log=file    Log file', copyright);
  log('', copyright);
  log('Display sample:', copyright);
  log('2020/02/11 21:59 08 (13:24)   read line 10000 K from mask2.20200211.08344941147.db', copyright);
  log('2020/02/11 21:59 20 (13:24) [  3                          ] Source 16198/26302, pack 2 (85.24%)  in 3 56, 5000 sources for 2290 M comp @ 9696 K c/s', copyright);
  log('2020/02/11 21:59 25 (13:24) 16235 / 26302 found 13574718 new pairs of sources in compdone*.db in 1 09. Must be reload for each source to get other computers work. Queuesize = 33 (min 5).', copyright);
  log('[  3] is the log of thread #3. Source 16198 is compared to 5000 other in pack 2, ie 2290M images compared. Global progress is 85%', copyright);
  log('When Queue size hit 0 then read disk is too long.', copyright);
  log('', copyright);
  log('Cancel the process by putting a file named stop.ask in image folder or by pressing ESC key and wait few minutes for clean up.', copyright);
  log('', copyright);
  log('************************************************************************************', 0);

end;

procedure ReadParams;
var
  i: integer;
begin
  debut      := Now;
  debug      := 1;
  flogname   := '';
  copyright  := 1;
  script     := '';
  threshold  := 10;
  pid        := TimeStamp(1);
  nbthreads  := 3;
  upgradedone:= false;
  clean      := false;
  firstload  := true;
  glob       := false;
  errimg     := 0;
  fdbexclude := '';
  stopaskedflag := False;

  folderimg := ParamStr(1);
  if RightStr(folderimg,1) <> '/' then folderimg := folderimg + '/';

  for i := 1 to ParamCount do begin
    p := ParamStr(i);
    if LeftStr(p,3)='-v='                   then debug := StrToInt(RightStr(p,length(p)-3));
    if LeftStr(p,5)='-log='                 then flogname := RightStr(p,length(p)-5);
    if LeftStr(p,3)='-s='                   then script := RightStr(p,length(p)-3);
    if LeftStr(p,3)='-t='                   then threshold := StrToInt(RightStr(p,length(p)-3));
    if LeftStr(p,3)='-nc'                   then copyright := 12;
    if LeftStr(p,12)='-upgradedone'         then upgradedone := true;
    if LeftStr(p,6)='-clean'                then clean := true;
    if LeftStr(p,5)='-glob'                 then glob := true;
    if LeftStr(p,9)='-threads='             then nbthreads := StrToInt(RightStr(p,length(p)-9));;
  end;

  if flogname <> '' then begin
    if FileExists(flogname) then DeleteFile(flogname);
    CreateFile(flogname);
  end;

  prthelp(copyright);

  if not(DirectoryExists(folderimg)) then begin
    log('ERROR: folderimg is mandatory and FileExists(' + folderimg + ') = ' + booltostr(FileExists(folderimg)), 0);
    halt;
  end;
  if nbthreads > high(Threads) then begin
    log('ERROR : Number of threads limited to ' + inttostr(high(Threads)) + '. You can modify this limit in LoopSources procedure.', 0);
    halt;
  end;

  if script = '' then begin
    i :=1;
    while fileexists(folderimg + 'compdone' + rightstr('00000' + inttostr(i), 6) + '.1.rs') do
      inc(i);
    script := 'compdone' + rightstr('00000' + inttostr(i), 6);
    CreateFile(folderimg + script + '.1.rs');
  end;

  log('debug=' + IntToStr(debug), 1);
  log('threshold=' + IntToStr(threshold), 1);
  log('threads=' + IntToStr(nbthreads), 1);
  log('log=' + flogname, 1);
  if upgradedone then begin
    log('', 1);
    log('UPGRADEDONE mode', 1)
  end else if clean then begin
    log('', 1);
    log('CLEAN mode', 1);
  end;
  log('', 0);
end;

begin
  {$if declared(UseHeapTrace)}
  GlobalSkipIfNoLeaks := true; // supported as of debugger version 3.2.0
  SetHeapTraceOutput('trace.log'); // supported as of debugger version 3.2.0
  {$ifend}

  ReadParams;

  filecount := 0;
  imgcount  := 0;
  firstsource := nil;
  t := Now;

  // Read all *.fp and store their content in memory : sources and image/key pairs
  if not(stopasked) then begin
    if glob then
      GlobScan(folderimg, filecount)
    else
      RecurseScan(folderimg, filecount);
    log('RecurseScan done in ' + DurationToStr(t, Now, 1) + ' and found ' + inttostr(imgcount) + ' pairs.', 1);
    InitIdx;
  end;
  if errimg>0 then begin
    log('Inconsistence in sources. Relaunch 1parse.py -clean', 1);
  end else
    if not(stopasked) then begin
      if clean then
        LoadDone(0, True)
      else begin
        log(inttostr(filecount) + ' sources, ' + unites(imgcount) + ' images and ' + unites(round(imgcount * imgcount / 2)) + ' comparison to perform', 1);
        loadpreviouscomputetime;
        LoopSources;
      end;
      log('FINISHED.', 1);
    end;
  closure;
end.

