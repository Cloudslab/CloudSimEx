#!/bin/bash

echo Current Dir: `pwd`
echo
echo

echo "=================== Killing all running java processes..."
sudo killall -9 java
sleep 5s
echo 
echo

echo "=================== Backing up previous results..."
for f in `ls multi-cloud/stat/ | egrep wld.*\.zip`
do
	echo $f
	if [ ! -d multi-cloud/stat/$f ]
	then
	    echo "	Backing up: "$f
	    cp -f multi-cloud/stat/$f multi-cloud/$f
	fi
done
sleep 5s
echo
echo 

echo "=================== Cleaning up old data..."
for dir in `ls multi-cloud/stat/ | grep wld-*`
do
	if [ -d multi-cloud/stat/$dir ]
	then
		echo "	Removing: "$dir
	    sudo rm -rf multi-cloud/stat/$dir
	fi
done
sleep 5s
echo
echo 

echo "=================== Removing previous binaries and output..."
sudo rm run.jar
sudo rm target/cloudsimex-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar
sudo rm out.txt
sleep 5s
echo
echo

echo "=================== Building project..."
cd ..
mvn clean install  | grep BUILD
cd ./cloudsimex-experiments
mvn clean install  | grep BUILD
mvn clean compile assembly:single | grep BUILD
sleep 5s
echo
echo

echo "=================== Running Experiments..."
mv -f target/cloudsimex-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar ./run.jar
sleep 5s
java -jar run.jar &> out.txt &





