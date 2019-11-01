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
}

const
  version: string = 'compare v2.0 b007.003';

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
  Threads: array[1..8] of TMyThread;  // Modify here to increase multi-threading limit
  Threadstatus: array[1..8] of integer; // and here. 0=free, 1= running, 2=writing

  idxDone: array of array of boolean;
  idxSource: array of TSource;

  folderimg, flogname: string;
  debug, threshold, copyright, mask, masksize, cptdisplay : integer;
  script, maskmethod: string;
  filecount, imgcount, nbthreads: integer;
  p, param, pid, lbl: string;
  firstsource: pSource;
  debut, LastLoading, t: TDateTime;
  clean, upgradedone, firstload: boolean;

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

function IndexSource(filename: string): integer;
var
  m, n, min, max: integer;
begin
  IndexSource := -1;
  min := 0;
  max := filecount;
  n := 0;
  repeat
    m := n;
    n := (min + max) div 2;

    if idxSource[n].filename > filename then max := n
    else min := n;

    if idxSource[n].filename = filename then IndexSource := n
    else if (n > 0) and (idxSource[n-1].filename = filename) then IndexSource := n-1
    // debug : filecount or filecount-1 ?
    else if (n < filecount) and (idxSource[n+1].filename = filename) then IndexSource := n+1
    else if abs(m - n) < 2 then begin
      IndexSource := 999999999;
      log('Not found in Index (n=' + inttostr(n) + ') "' + filename + '"', 3);
      //log('filecount=' + inttostr(filecount) + ', min-max=' + inttostr(min) + '-' + inttostr(max), 2);
      //if (n>0) then log('  Closest #' + inttostr(n-1) + ' "' + idxSource[n-1].filename +'"', 2);
      //if (n<filecount) then log('  Closest #' + inttostr(n+1) + ' "' + idxSource[n+1].filename +'"', 2);
    end;

  until IndexSource > -1;
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
begin
  fold:=ExpandFileName(fold);
  if fold[length(fold)]='/' then fold := leftstr(fold,length(fold)-1);
  if fold <> folderimg + 'unwanted' then begin
    Log('Load fingerprints from ' + fold, 4);
    if (filecount = cptdisplay) then begin
      if cptdisplay > 0 then
        log('Loaded ' + inttostr(filecount) + ' images folders...', 1);
      cptdisplay := cptdisplay + 1000;
    end;
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

                //log('f closed', 3, false);
              end else begin
                log('ERROR loading : ' + fullname, 0);
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

function distanceham(key1, key2: TKey): integer;
begin
  distanceham := PopCnt(key1[3] xor key2[3]) + PopCnt(key1[2] xor key2[2]) + PopCnt(key1[1] xor key2[1]) + PopCnt(key1[0] xor key2[0]);
end;

function lock(lockfile, pid, kind: string): boolean;
//kind=fast set lock without control, ctrl=after a fast perform the control, full=fast+ctrl
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
  if todo and FileExists(lockfile + '.done') then
    if CompareDateTime(FileDateToDateTime(FileAge(lockfile + '.done')), debut) < 0 then begin
      log(lockfile + '.done exists and it is old. Lock should be set.', 2);
    end else begin
      log(lockfile + '.done exists and it is recent. Lock not to be set.', 2);
      todo := False;
    end;

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
  line,lstr,rstr, dbf: string;
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

          i := IndexSource(lstr);
          j := IndexSource(rstr);

          if (i <> 999999999) and (j <> 999999999) then begin
            inc(nb);
            idxDone[i][j] := True;
          end else begin
            inc(notfound);
            if i=999999999 then
              log('-- Not found but in ' + result.Name + ' : ' + lstr, 2)
            else
              log('-- Not found but in ' + result.Name + ' : ' + lstr, 2)
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
  if notfound > 0 then
    log(inttostr(position) + ' / ' + inttostr(filecount) + ' found ' + inttostr(nb) + ' new pairs of sources in compdone*.db in ' + DurationToStr(t, Now, 1) + '. Must be reload for each source to get other computers work. ' + inttostr(notfound) + ' NOT FOUND ', i)
  else
    log(inttostr(position) + ' / ' + inttostr(filecount) + ' Found ' + inttostr(nb) + ' new pairs of sources in compdone*.db in ' + DurationToStr(t, Now, 1) + '. Must be reload for each source to get other computers work.', i);
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

  //open & close file cannot be outside the loop because killing the program will let results not written to disk
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
  //log(txtrs, 1);
  write(frs,txtrs);
  close(frs);

  CreateFile(folderimg + lbl + '.' + pid + inttostr(FAthreadnb) + '.db');
  assignfile(f, folderimg + lbl + '.' + pid + inttostr(FAthreadnb) + '.db');
  append(f);
  for cpt := 1 to FARight.count do begin
    if idxSource[FAleft].filename < idxSource[FAright.data[cpt]].filename then
      line := idxSource[FAleft].filename + ' ; ' + idxSource[FAright.data[cpt]].filename
    else
      line := idxSource[FAright.data[cpt]].filename + ' ; ' + idxSource[FAleft].filename;
    writeln(f, line);
  end;
  close(f);
  //DeleteFile(dbf + '.run');

  endleft := Now;
  durms := duration(beginleft, endleft);
  log(locscript2 + FAcomment + ' in ' + DurationToStr(beginleft, endleft, 1) + ', '
    + unites(FARight.count) + ' sources for ' + unites(FARight.imcount * idxSource[FAleft].imgcount) + ' comp @ '
    + unites(1000 * FARight.imcount * idxSource[FAleft].imgcount / durms) + ' c/s', 1);
  //+ unites(FARight.imcount) + ' images, '
  Threadstatus[FAthreadnb] := 0;
  FAFinished := True;
end;

procedure LoopSources;
var
  sleft, sright: integer;
  spack: TPack;
  i, nbpack: integer;
  lockfile, prevlockfile, lockctrl: string;
  ThreadLaunched : Boolean;
  t1: TDateTime;

  procedure ThreadWait;
  var
    i, nbactive: integer;
  begin
    repeat
      sleep(500);
      nbactive := 0;
      for i:=1 to nbthreads do
        if Threadstatus[i] > 0 then
          inc(nbactive);
    until nbactive=0;
  end;

  procedure ThreadExec(comment: string);
  var
    i: integer;
  begin
    ThreadLaunched := False;
    repeat
      for i:=1 to nbthreads do begin
        if not(ThreadLaunched) and (Threadstatus[i] = 0) then begin
          Threadstatus[i] := 1;
          Threads[i]:=TMyThread.Create(True);
          Threads[i].FreeOnTerminate := True;
          Threads[i].ALeft           := sleft;
          Threads[i].ARight          := spack;
          Threads[i].Athreadnb       := i;
          Threads[i].Acomment        := comment;
          Threads[i].Start;
          ThreadLaunched := True;
        end;
      end;
      sleep(500);
    until ThreadLaunched;
  end;

begin
  if nbthreads > high(Threads) then begin
    log('ERROR : Number of threads limited to ' + inttostr(high(Threads)) + '. You can modify this limit in LoopSources procedure.', 0);
    halt;
  end;
  for i := 1 to nbthreads do Threadstatus[i] := 0;
  prevlockfile := '';

  for sleft :=0 to filecount-1 do begin
    t1 := Now;
    lockfile := folderimg + idxSource[sleft].midpath + idxSource[sleft].filename + '.compare.' + param;
    if lock(lockfile, pid, 'fast') then begin
      lockctrl := 'not tested';
      nbpack := 0;
      spack.count   := 0;
      spack.imcount := 0;
      loaddone(sleft + 1, False);
      //t3 := Now;

      for sright := 0 to filecount-1 do begin
        // Loop all right not only when sright>sleft. Then double check in idxDone
        if not(idxDone[sleft][sright]) and not(idxDone[sright][sleft]) and (sleft<>sright) then begin
          if lockctrl = 'not tested' then
            if lock(lockfile, pid, 'ctrl') then lockctrl := 'ok' else lockctrl := 'exit';
          if lockctrl = 'ok' then begin
            //if nbpack = 1 then
            //  log('Start comparing ' + inttostr(sleft+1) + ' / ' + inttostr(filecount), 1);
            log('START COMP ' + inttostr(sleft+1) + ' / ' + inttostr(filecount) + ', ' + inttostr(sleft) + ' vs ' + inttostr(sright) + ' : ' + idxSource[sleft].filename + ' ; ' + idxSource[sright].filename, 3);
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
        //end else begin
        //  log('SKIP (idxDone) ' + inttostr(sleft+1) + ' / ' + inttostr(filecount) + ', ' + inttostr(sleft) + ' vs ' + inttostr(sright) + ' : ' + idxSource[sleft].filename + ' ; ' + idxSource[sright].filename, 4);
        end;
      end;
      //log('  ALL right source in ' + DurationToStr(t3, Now, 1), 3);
      if (spack.count > 0) and (lockctrl = 'ok') then begin
        inc(nbpack);
        ThreadExec('Source ' + inttostr(sleft+1) + '/' + inttostr(filecount) + ', last pack ' + inttostr(nbpack) + ' (' + formatfloat('0.00',100 * (1 - sqr(filecount-sleft) / sqr(filecount))) + '%) ');
      end;

      if (lockctrl = 'ok') and FileExists(prevlockfile + '.run') then begin
        try
          log('Finished. Renaming ' + prevlockfile + '.run to .done.', 2);
          if FileExists(prevlockfile + '.done') then begin
            DeleteFile(prevlockfile + '.done');
            sleep(100);
          end;
          RenameFile(prevlockfile + '.run', prevlockfile + '.done');
        except
          log('Cannot rename ' + prevlockfile + '.run to *.done', 1);
        end;
      end;
      prevlockfile := lockfile;
    end else begin
      log('Lock reserved by another process', 3);
    end;
    log('1 left source in ' + DurationToStr(t1, Now, 1), 3);
  end;
  // Main program finished: no more data to process
  ThreadWait;
  if (lockctrl = 'ok') and FileExists(prevlockfile + '.run') then begin
    try
      log('Finished. Renaming ' + prevlockfile + '.run to .done.', 2);
      if FileExists(prevlockfile + '.done') then begin
        DeleteFile(prevlockfile + '.done');
        sleep(100);
      end;
      RenameFile(prevlockfile + '.run', prevlockfile + '.done');
    except
      log('Cannot rename ' + prevlockfile + '.run to *.done', 1);
    end;
  end;
end;

procedure DoneToDB;
// Use to upgrade previous version with .done file in each image folder
// to new version with compdone.db at root image folder.
var
  pts, ptsright: pSource;
  sleft, sright: TSource;
  donewitholdcode: boolean;
  lockfile: string;
  nbsl: integer;
  f: Textfile;
  dbf, line: string;

begin
  log('Begin update done to DB of ' + inttostr(filecount) + ' source files.', 1);
  nbsl := 0;
  dbf := folderimg + lbl + '.compdone.db';
  try
    CreateFile(dbf);
    assignfile(f, dbf);
    append(f);
  except
    log('ERROR create file ' + dbf + ', try another name.', 0);
    dbf := folderimg + lbl + '.compdone1.db';
    CreateFile(dbf);
    sleep(1000);
    assignfile(f, dbf);
    append(f);
  end;

  pts := firstsource;
  repeat
    sleft := pts^;
    inc(nbsl);
    log('Start ' + inttostr(nbsl) + ' / ' + inttostr(filecount), 3);
    lockfile := folderimg + sleft.midpath + sleft.filename + '/compare.' + param;
    //lockfile := folderimg + sleft.filename + '/compare.' + param;
    donewitholdcode := FileExists(lockfile + '.done');
    if donewitholdcode then begin
      log('FileExists ' + lockfile + '.done', 2);
      log(sleft.filename + ' done with old code. Start loop to update DB.', 2);

      ptsright := firstsource;
      repeat
        sright := ptsright^;
        if sleft.filename < sright.filename then begin
          line := sleft.filename + ' ; ' + sright.filename;
          writeln(f, line);
        end;
        ptsright := sright.next;
      until (ptsright = nil);

    end;
    pts := sleft.next;
  until (pts = nil);

  close(f);
  log('Update finished', 0);
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
  log('-upgradedone From v1 to v2, from *.done to *.db', copyright);
  log('-clean       Read all DB files, remove references to old files, remove duplicates, store all in 1 file.', copyright);
  //log('-mask=n      To limit the comparison to some images files for each source file. 1/n images are used. Performance impact.', copyright);
  //log('-masksize=n  Read n images per source then skip (mask-1)*n images', copyright);
  //log('-maskmethod= cycle: read n images per source then skip (mask-1)*masksize images, random: if random read masksize images else skip maxsize images.', copyright);
  log('-log=file    Log file', copyright);
  log('', copyright);
  log('Display sample:', copyright);
  log('[  3] Source 3 / 462 done. 1518 M comparison in 2''54". Thread perf = 8710 K comp/sec. 1860 images @ 10.68 i/s', copyright);
  log('ETA this computer 21:33''55"', copyright);
  log('I used -s=maskfptest and -threads=3.', copyright);
  log('[  3] is the log of thread #3. Source #3 required 1518M key pairs comparison. The performance of 1 thread is about 8710K comparison per second.', copyright);
  log('ETA considering 3 threads, ie 3 * 8710K comp/sec will be 21 about hours.', copyright);
  log('Estimations are not accurate due to variances (data in cache, other process somehow limiting disk, ram or cpu...)', copyright);
  log('For distributed computing you have to adjust ETA. e.g. computer #1 say 20h, #2 say 30h and #3 say 40h, then ETA=1/(1/20+1/30+1/40)', copyright);
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
  //testskey1  := '';
  //testskey2  := '';

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
    //if LeftStr(p,6)='-mask='                then mask := StrToInt(RightStr(p,length(p)-6));
    //if LeftStr(p,10)='-masksize='           then masksize := StrToInt(RightStr(p,length(p)-10));
    //if LeftStr(p,12)='-maskmethod='         then maskmethod := RightStr(p,length(p)-12);
    if LeftStr(p,12)='-upgradedone'         then upgradedone := true;
    if LeftStr(p,6)='-clean'                then clean := true;
    if LeftStr(p,9)='-threads='             then nbthreads := StrToInt(RightStr(p,length(p)-9));;
    //if LeftStr(p,4)='-k1='                  then testskey1 := RightStr(p,length(p)-4);
    //if LeftStr(p,4)='-k2='                  then testskey2 := RightStr(p,length(p)-4);
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
  //log('copyright=' + IntToStr(copyright), 1);
  //log('mask=' + IntToStr(mask), 1);
  //log('maskmethod=' + maskmethod, 1);
  //log('masksize=' + IntToStr(masksize), 1);
  log('script=' + script, 1);
  log('lbl=' + lbl, 1);
  log('threads=' + IntToStr(nbthreads), 1);
  log('log=' + flogname, 1);
  //log('testskey1=' + testskey1, 1, false);
  //log('testskey2=' + testskey2, 1, false);
  if upgradedone then begin
    log('', 1);
    log('UPGRADEDONE mode', 1)
  end else if clean then begin
    log('', 1);
    log('CLEAN mode', 1);
  end;
  log('', 0);

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
end;

begin
  {$if declared(UseHeapTrace)}
  GlobalSkipIfNoLeaks := true; // supported as of debugger version 3.2.0
  SetHeapTraceOutput('trace.log'); // supported as of debugger version 3.2.0
  {$ifend}

  ReadParams;

  if upgradedone then begin
    log('Upgrade *.done sources to DB pairs.', 1);
    log('DO NOT EXECUTE IT WHEN OTHER INSTANCES ARE RUNNING', 1);
  end;

  filecount := 0;
  imgcount  := 0;
  firstsource := nil;
  t := Now;

  // Read all *.fp and store their content in memory : sources and image/key pairs
  RecurseScan(folderimg, filecount);
  log('RecurseScan done in ' + DurationToStr(t, Now, 1), 1);
  InitIdx;

  if upgradedone then
    donetodb
  else if clean then
    LoadDone(0, True)
  else begin
    log(inttostr(filecount) + ' sources, ' + unites(imgcount) + ' images and ' + unites(round(imgcount * imgcount / 2)) + ' comparison to perform', 1);
    LoopSources;
  end;
  log('FINISHED.', 1);
end.

