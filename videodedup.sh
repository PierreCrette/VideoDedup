./1parse.py /home/pierre/NAS_pierre/Video /home/pierre/NAS_pierre/nassys/img/db -i -d -f10

rm result.sh
echo '***************************************************************************************************'
echo findimagedupes -R --threshold=98 -f=/home/pierre/NAS_pierre/nassys/img/fingerprints --prune --include-file=template.sh --script=result.sh /home/pierre/NAS_pierre/nassys/img/db
findimagedupes -R --threshold=98 -f=/home/pierre/NAS_pierre/nassys/img/fingerprints --prune --include-file=template.sh --script=result.sh /home/pierre/NAS_pierre/nassys/img/db
echo '***************************************************************************************************'

chmod +x result.sh
rm doublonset.txt
./result.sh

./2analyse.py /home/pierre/NAS_pierre/Video /home/pierre/NAS_pierre/nassys/img doublonset.txt 2

