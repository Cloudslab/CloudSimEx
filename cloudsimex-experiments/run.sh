#!/bin/bash

echo Current Dir: `pwd`
echo
echo

echo "========= Killing all running java processes..."
sudo killall -9 java
echo 
echo
echo

echo "========= Cleaning up old data..."
for dir in `ls multi-cloud/stat/ | grep wld-*`
do
	echo "	Removing: "$dir
	if [ -d multi-cloud/stat/$dir ]
	then
	    sudo rm -rf multi-cloud/stat/$dir
	fi
done
echo
echo 

echo "========= Removing previous binaries and output..."
sudo rm run.jar
sudo rm target/cloudsimex-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar
sudo rm out.txt
echo
echo

echo "Building project..."
cd ..
mvn clean install 
cd ./cloudsimex-experiments
echo 
echo

mvn clean install
mvn clean compile assembly:single
echo
echo

echo "Running Experiments..."
mvn clean compile assembly:single 
mv target/cloudsimex-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar ./run.jar
java -jar run.jar &> out.txt &





