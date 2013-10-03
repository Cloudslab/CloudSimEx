#!/bin/bash

echo "=================== Experiment status ......"
sLen=15
eLen=5 
if [ ! -z "$1" ]
  then
    sLen=$1
fi

if [ ! -z "$2" ]
  then
    eLen=$2
fi



for dir in `ls multi-cloud/stat/ | grep wld-*`
do
	if [ -d multi-cloud/stat/$dir ]
	then
	    echo "============================================================================================ "
	    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> - " $dir " - <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< "
	    
	    echo "Files:"
	    for f in `ls multi-cloud/stat/$dir | egrep "\.log$|\.csv$"`
	    do
		echo "   " `ls -lh multi-cloud/stat/$dir/$f | cut -c 13-120` 
	    done

	    echo
	    echo
	    head -n $sLen multi-cloud/stat/$dir/MultiCloudFramework.log
	    echo "      ....       "
	    echo
	    tail -n $eLen multi-cloud/stat/$dir/MultiCloudFramework.log
	    echo 
	    echo
	    echo
	fi
done




