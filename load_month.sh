#!/bin/bash

# Clean files missing tons of data
find . -name "*.bars" -size -512kb -exec rm {} \;
# Month is of a format 201801
MONTH=$1
SYM=$2
for i in {01..31}; do
    DAY=$i
    if [ $i -lt 10 ]; then
        DAY="0${i}"
    fi
    DATE="${MONTH}${DAY}"
    WC=`find . -name "*$DATE*" | grep $SYM | wc -l`
    if [ $WC -eq 0 ]; then
        day=$(date -j -f "%Y%m%d" "$DATE" +"%u")
        if ((day > 5)); then
            echo "Skipping weekend $DATE"
        else
            echo "Processing $DATE"
            python main.py ${SYM} $DATE
        fi
    else
        echo "Skipping $DATE"
    fi
done
    


echo $day
if ((day > 5)); then
       echo "WEEKEND"        
   else
          echo "WORKING DAY"
      fi
