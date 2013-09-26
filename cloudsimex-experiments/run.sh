#!/bin/bash

echo Current Dir: `pwd`
echo
echo

echo "Killing all running java processes..."
killall -9 java
echo 
echo
echo

echo "Cleaning up old data..."
for dir in `ls multi-cloud/stat/ | grep wld-*`
do
	echo "	Removing: "$dir
	rm -rf multi-cloud/stat/$dir
done
echo
echo
echo 

echo "Removing previous binaries and output..."
rm run.jar
rm target/cloudsimex-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar
rm out.txt
echo
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
echo


echo "Running Experiments..."
mvn clean compile assembly:single 
mv target/cloudsimex-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar ./run.jar
java -jar run.jar &> out.txt &





