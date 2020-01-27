#split -a 3 -l 6000000 tu7db.20191226.rs rsdb


ls /zpool/NAS/img/img.1-not-saved/rs/in/* | head -n 1 | ./3j_analyse.py /zpool/NAS/media /zpool/NAS/img/img.1-not-saved {} -v=1 -threads=1 -uwfp

ls /zpool/NAS/img/img.1-not-saved/rs/in/* | parallel ./3j_analyse.py /zpool/NAS/media /zpool/NAS/img/img.1-not-saved {} -v=1 -threads=1 -t=5 -tu=7 -maxdiff=10 -hdmaxdiff=50 -out={}.out -fake




