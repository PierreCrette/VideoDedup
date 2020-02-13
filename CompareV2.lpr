program CompareV2;

{$mode objfpc}{$H+}
uses
{$ifdef unix}
  cthreads,
  cmem, // the c memory manager is on some systems much faster for multi-threading
  Classes,

{$endif}
  //Crt, IniFiles, Process, SysCall
  SysUtils, Math, DateUtils;

{$OPTIMIZATION LEVEL3}
{$rangeChecks on}

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

Performance on img.2 dataset with 26307 images:
- After reboot :             load images in 2h08
- Cached :                   load images in 7mn
- glob, after reboot :       load images in 7mn
- glob, cached :             load images in

}

const
  version: string = 'compare v2.0.3.3 20200213';

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
    imgcount   : integer;
    next       : pSource;
    end;
  TPack = record
    count  : integer;
    imcount: integer;
    data   : array[1..5000] of integer;
    end;
  Tqueuelt = record
    qleft    : integer;
    qpack    : TPack;
    qcomment : string;
  end;

  TMyThread = class(TThread)
  private
    FAFinished: boolean;
    FALeft:     integer;
    FARight:    TPack;
    FAthreadnb: integer;
    FAcomment:  string;
  public
    procedure Execute; override;
    property AFinished: boolean read FAFinished write FAFinished;
    property ALeft:     integer read FALeft write FALeft;
    property ARight:    TPack read FARight write FARight;
    property Athreadnb: integer read FAthreadnb write FAthreadnb;
    property Acomment:  string read FAcomment write FAcomment;
  end;

var
  Threads: array[1..128] of TMyThread;  // Modify here to increase multi-threading limit
  Threadstatus: array[1..128] of integer; // and here. 0=free, 1= running, 2=writing
  queue: array[0..256] of Tqueuelt;
  queuelen, queuemin: integer;

  idxDone: array of array of boolean;
  idxSource: array of TSource;

  folderimg, flogname: string;
  debug, threshold, copyright, mask, masksize, cptdisplay : integer;
  script, maskmethod: string;
  filecount, imgcount, nbthreads: integer;
  p, param, pid, lbl: string;
  firstsource: pSource;
  debut, LastLoading, t: TDateTime;
  clean, glob, upgradedone, firstload: boolean;

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
  if n > 10000000000000 then begin
    tmp := n / 1000000000000;
    u := ' T';
  end else begin
    if n > 10000000000 then begin
      tmp := n / 1000000000;
      u := ' G';
    end else begin
      if n > 10000000 then begin
        tmp := n / 1000000;
        u := ' M';
      end else begin
        if n > 10000 then begin
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

procedure Log(s: string; verbose:integer); //inthread: boolean
var
  flog: TextFile;
  output: string;
begin
  output := TimeStamp(0) + s;
  if flogname <> '' then begin
    if FileExists(flogname) then begin
      assignfile(flog, flogname);
      append(flog);
      writeln(flog, output);
      close(flog);
    end else begin
      assignfile(flog, flogname);
      rewrite(flog);
      writeln(flog, output);
      close(flog);
    end;
  end;
  if verbose <= debug then writeln(output);
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
      log('InitIdx: ERROR 2 files with same name. One is discarded. Relaunch 1f_Parse.py -clean to solve.' + ps^.filename + ' and ' + ps^.next^.filename, 0);
      ps^.next := ps^.next^.next;
      dec(filecount);
    end;
    ps := ps^.next
  end;

  //Populate idxDone as a square matrix of done/todo video pairs
  ps := firstsource;
  for i:=0 to filecount-1 do begin
    if ps <> nil then begin
      idxSource[i] := ps^;

      log('InitIdx[' + inttostr(i) + '] = ' + idxSource[i].filename, 3);

      for j:=0 to filecount-1 do
        idxDone[i][j] := false;
      ps := ps^.next;
    end else begin
      log('ERROR InitIdx ps = nil', 1);
    end;
  end;
  log('InitIdx done in ' + DurationToStr(t, Now, 1), 1);
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
              if (LeftStr(result.Name, 4) = 'glob') and glob then begin
                filecount := strtoint(copy(result.Name, 5, length(result.Name) - 7));
                log('Loading ' + fullname + ' with ' + inttostr(filecount) + ' image folders', 1);
                OneFP;
              end;
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
  log('Lock ' + lockfile, 3);
  if (kind<>'full') and (kind<>'fast') and (kind<>'ctrl') then begin
    log('ERROR kind of lock : ' + kind,1);
    halt;
  end;
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
  log('Lock in ' + DurationToStr(t, Now, 1), 3);
end;

procedure LoadDone(position: integer; clean: boolean);
var
  f: Textfile;
  result: TRawByteSearchRec;
  line,lstr,rstr, dbf, s: string;
  i, j: integer;
  nb, delim, notfound: integer;
  newfile: boolean;
  t: TDateTime;
begin
  t := Now;
  if firstload then log('Firstload of previously done comparison...', 1);
  nb := 0;
  notfound := 0;
  if FindFirst(folderimg + lbl + '*.db', faAnyFile and faDirectory, result)=0 then begin
    repeat
      //log('Found : ' + result.Name, 2);
      newfile := (CompareDateTime(FileDateToDateTime(FileAge(folderimg + result.Name)), LastLoading) > 0);
      if (result.Name <> '.') and (result.Name <> '..') and ((result.Attr and faDirectory) <> faDirectory) and (newfile or firstload) then begin
        assignfile(f,folderimg + result.Name);
        reset(f);
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
              log('-- Not found but in ' + result.Name + ' : ' + lstr, 2)
            else
              log('-- Not found but in ' + result.Name + ' : ' + rstr, 2)
          end;

          if ((nb + notfound) mod 10000000 = 0) then
            log('  read line ' + unites(nb + notfound) + ' from ' + result.Name, 1);

        end;
        close(f);
      end;
    until FindNext(result)<>0;
    FindClose(result);
  end;

  if clean then begin
    // Write new DB without inconsistencies
    dbf := lbl + TimeStamp(1) + '.db';
    CreateFile(folderimg + dbf);
    assignfile(f, folderimg + dbf);
    append(f);
    for i:=0 to filecount-2 do
      for j:=i+1 to filecount-1 do
        if idxDone[i][j] or idxDone[j][i] then
          writeln(f, idxSource[i].filename + ' ; ' + idxSource[j].filename);
    close(f);
    log('Created ' + dbf, 1);
    // Delete old DB files
    if FindFirst(folderimg + lbl + '*.db', faAnyFile and faDirectory, result)=0 then begin
      repeat
        if (result.Name <> '.') and (result.Name <> '..') and ((result.Attr and faDirectory) <> faDirectory) and (result.Name <> dbf) then
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
  if (nb > 0) or (position mod 100 = 0) then i:=1 else i:=2;
  s := inttostr(position) + ' / ' + inttostr(filecount) + ' found ' + inttostr(nb) + ' new pairs of sources in compdone*.db in ';
  s := s + DurationToStr(t, Now, 1) + '. Must be reload for each source to get other computers work. Queuesize = ';
  s := s + inttostr(queuelen) + ' (min ' + inttostr(queuemin) + '). ';
  if notfound > 0 then s := s + inttostr(notfound) + ' NOT FOUND ';
  log(s, 1);
  firstload := false;
end;

procedure TMyThread.Execute;
var
  pti, ptiright: pFingerprint;
  iright: TFingerprint;
  ileft: TFingerprint;
  frs: TextFile;
  r, cpt: integer;
  locscript, locscript2: string;
  beginleft, endleft : TDateTime;
  durms: Comp;
  f: Textfile;
  line, txtrs: string;

begin
  beginleft := Now;
  FAFinished := False;
  locscript  := folderimg + script + '.' + inttostr(FAthreadnb);
  locscript2 := '[';
  for r :=1 to nbthreads do
    if r = FAthreadnb then
      locscript2 := locscript2 + inttostr(FAthreadnb)
    else
      locscript2 := locscript2 + ' ';
  locscript2 := locscript2 + '] ';
  CreateFile(locscript + '.rs');
  log(locscript2 + 'Begin Thread #' + inttostr(FAthreadnb) + ' ' + FAcomment + ' with count = ' + inttostr(FARight.count) + ', imcount = ' + inttostr(FARight.imcount)
  + ', for ' + inttostr(idxSource[FALeft].imgcount) + ' left images.', 2);

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
          //log(locscript2 + '--- found a similarity', 4);
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
  log(locscript2 + '--- write resultset to disk', 4);
  assignfile(frs, locscript + '.rs');
  append(frs);
  write(frs,txtrs);
  close(frs);

  CreateFile(folderimg + lbl + '.' + pid + '.' + inttostr(FAthreadnb) + '.' + inttostr(FAleft + 1) + '.db');
  assignfile(f, folderimg + lbl + '.' + pid + '.' + inttostr(FAthreadnb) + '.' + inttostr(FAleft + 1) + '.db');
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
  log(locscript2 + FAcomment + ' in ' + DurationToStr(beginleft, endleft, 1) + ', '
    + unites(FARight.count) + ' sources for ' + unites(FARight.imcount * idxSource[FAleft].imgcount) + ' comp @ '
    + unites(1000 * FARight.imcount * idxSource[FAleft].imgcount / durms) + ' c/s', 1);
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
            if queuelen < queuemin then queuemin := queuelen;
            if queuemin = 0 then queuemin := 2 * nbthreads;
            dec(queuelen);
            inc(nbactive);
            Threads[i].ALeft    := queue[queuelen].qleft;
            Threads[i].ARight   := queue[queuelen].qpack;
            Threads[i].Acomment := queue[queuelen].qcomment;
            Threads[i].Start;
          end;
    until nbactive=0;
  end;

  procedure ThreadExec(comment: string);
  var
    i: integer;
    qelt: Tqueuelt;
  begin
    ThreadLaunched := False;
    ThreadQueued   := False;
    qelt.qleft     := sleft;
    qelt.qpack     := spack;
    qelt.qcomment  := comment;
    repeat
      for i:=1 to nbthreads do begin
        if not(ThreadLaunched) and (Threadstatus[i] = 0) then begin
          Threadstatus[i] := 1;
          Threads[i] := TMyThread.Create(True);
          Threads[i].FreeOnTerminate := True;
          Threads[i].Athreadnb       := i;
          if queuelen > 0 then begin
            if queuelen < queuemin then queuemin := queuelen;
            //if queuemin = 0 then queuemin := 2 * nbthreads;
            dec(queuelen);
            Threads[i].ALeft    := queue[queuelen].qleft;
            Threads[i].ARight   := queue[queuelen].qpack;
            Threads[i].Acomment := queue[queuelen].qcomment;
          end else begin
            Threads[i].ALeft    := qelt.qleft;
            Threads[i].ARight   := qelt.qpack;
            Threads[i].Acomment := qelt.qcomment;
            ThreadLaunched := True;
          end;
          Threads[i].Start;
        end;
      end;
      if not(ThreadLaunched) then begin
        if queuelen < (2 * nbthreads) then begin
          queue[queuelen] := qelt;
          ThreadQueued    := True;
          inc(queuelen);
        end;
      end;
      if not(ThreadLaunched or ThreadQueued) then begin
        log('Queue full. Waiting.', 2);
        sleep(2000);
      end;
    until ThreadLaunched or ThreadQueued;
  end;

begin
  if nbthreads > high(Threads) then begin
    log('ERROR : Number of threads limited to ' + inttostr(high(Threads)) + '. You can modify this limit in LoopSources procedure.', 0);
    halt;
  end;
  for i := 1 to nbthreads do Threadstatus[i] := 0;
  prevlockfile   := '';
  queuelen  := 0;
  queuemin  := 2 * nbthreads;

  for sleft := 0 to filecount-1 do begin
    t1 := Now;
    lockfile := folderimg + idxSource[sleft].midpath + idxSource[sleft].filename + '.compare.' + param;
    if lock(lockfile, pid, 'fast') then begin
      lockctrl := 'not tested';
      nbpack := 0;
      spack.count   := 0;
      spack.imcount := 0;
      t2 := Now;
      loaddone(sleft + 1, False);
      durload := DurationToStr(t2, Now, 1);
      for sright := 0 to filecount-1 do begin
        // Loop all right not only when sright>sleft. Then double check in idxDone
        if not(idxDone[sleft][sright]) and not(idxDone[sright][sleft]) and (sleft<>sright) then begin
          if lockctrl = 'not tested' then
            if lock(lockfile, pid, 'ctrl') then lockctrl := 'ok' else lockctrl := 'exit';
          if lockctrl = 'ok' then begin
            //log('START COMP ' + inttostr(sleft+1) + ' / ' + inttostr(filecount) + ', ' + inttostr(sleft) + ' vs ' + inttostr(sright) + ' : ' + idxSource[sleft].filename + ' ; ' + idxSource[sright].filename, 2);
            inc(spack.count);
            spack.imcount := spack.imcount + idxSource[sright].imgcount;
            spack.data[spack.count] := sright;
            if (spack.imcount * idxSource[sleft].imgcount > 5000000000) or (spack.count = high(spack.data)) then begin
              inc(nbpack);
              ThreadExec('Source ' + inttostr(sleft+1) + '/' + inttostr(filecount) + ', pack ' + inttostr(nbpack) + ' (' + formatfloat('0.00',100 * (1 - sqr(filecount-sleft) / sqr(filecount))) + '%) ');
              spack.count   := 0;
              spack.imcount := 0;
            end;
          end;
        end;
      end;
      if (spack.count > 0) and (lockctrl = 'ok') then begin
        inc(nbpack);
        ThreadExec('Source ' + inttostr(sleft+1) + '/' + inttostr(filecount) + ', last pack ' + inttostr(nbpack) + ' (' + formatfloat('0.00',100 * (1 - sqr(filecount-sleft) / sqr(filecount))) + '%) ');
      end;

      if (lockctrl = 'ok') and FileExists(prevlockfile + '.run') then begin
        try
          log('Finished. Removing ' + prevlockfile + '.run', 3);
          DeleteFile(prevlockfile + '.run');
        except
          log('Cannot remove ' + prevlockfile + '.run', 1);
        end;
      end;
      prevlockfile := lockfile;
      log('Duration to prepare source #' + inttostr(sleft) + ' is ' + DurationToStr(t1, Now, 1) + ', including ' + durload + ' for loading updated .db (+ wait if queue is full)', 2);
    end else begin
      log('Lock reserved by another process', 3);
    end;
    log('1 left source in ' + DurationToStr(t1, Now, 1), 3);
  end;
  // Main program finished: no more data to process
  ThreadWait;
  if (lockctrl = 'ok') and FileExists(prevlockfile + '.run') then begin
    try
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
  log('-lbl=label   Label to identify runs with different parameters. Use the same on all sessions/computers to share workload. No special characters since its use for file naming.', copyright);
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
  lbl        := '';
  threshold  := 10;
  mask       := 1;
  maskmethod := 'cycle';
  masksize   := 1;
  pid        := TimeStamp(1);
  nbthreads  := 3;
  upgradedone:= false;
  clean      := false;
  firstload  := true;
  glob       := false;

  folderimg := ParamStr(1);
  if RightStr(folderimg,1) <> '/' then folderimg := folderimg + '/';

  for i := 1 to ParamCount do begin
    p := ParamStr(i);
    if LeftStr(p,3)='-v='                   then debug := StrToInt(RightStr(p,length(p)-3));
    if LeftStr(p,5)='-log='                 then flogname := RightStr(p,length(p)-5);
    if LeftStr(p,3)='-s='                   then script := RightStr(p,length(p)-3);
    if LeftStr(p,5)='-lbl='                 then lbl := RightStr(p,length(p)-5);
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

  if not(FileExists(folderimg)) then begin
    log('ERROR: folderimg is mandatory.', 0);
    exit;
  end;
  if lbl = '' then begin
    log('ERROR: parameter -lbl is mandatory to trace work done.', 0);
    exit;
  end;
  if nbthreads > high(Threads) then begin
    log('ERROR : Number of threads limited to ' + inttostr(high(Threads)) + '. You can modify this limit in LoopSources procedure.', 0);
    halt;
  end;

  if script = '' then begin
    i :=1;
    while fileexists(folderimg + lbl + rightstr('00000' + inttostr(i), 6) + '.1.rs') do
      inc(i);
    script := lbl + rightstr('00000' + inttostr(i), 6);
    CreateFile(folderimg + script + '.1.rs');
  end;

  param := lbl + '_t_' + IntToStr(threshold) + '_' + maskmethod + '_' + IntToStr(mask) + '_' + IntToStr(masksize);
  log('param = ' + param, 1);

  log('debug=' + IntToStr(debug), 1);
  log('threshold=' + IntToStr(threshold), 1);
  log('script=' + script, 1);
  log('lbl=' + lbl, 1);
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
  RecurseScan(folderimg, filecount);
  log('RecurseScan done in ' + DurationToStr(t, Now, 1) + ' and found ' + inttostr(imgcount) + ' pairs.', 1);
  InitIdx;

  if clean then
    LoadDone(0, True)
  else begin
    log(inttostr(filecount) + ' sources, ' + unites(imgcount) + ' images and ' + unites(round(imgcount * imgcount / 2)) + ' comparison to perform', 1);
    LoopSources;
  end;
  log('FINISHED.', 1);
end.

