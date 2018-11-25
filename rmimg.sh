echo "-----------------------------------------------------"
cat toremove.txt
echo "-----------------------------------------------------"
read -p "Les fichiers ci-dessus vont être effacés..."
echo " "

while read p; do
  rm "$p"
done <toremove.txt

echo " "
echo "done"




