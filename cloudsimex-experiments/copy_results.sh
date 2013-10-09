#!/bin/bash

path=~/Dev/Eclipse_WS_Uni/cloudsimex/cloudsimex-experiments/multi-cloud/stat
localPath=./multi-cloud/
sshTerm=nikolay@128.250.192.43

if [ ! -z "$1" ]
  then
    if [ $1 == "ec2" ]
    then
	sshTerm="-i ~/n-kp.pem ubuntu@ec2-54-252-52-215.ap-southeast-2.compute.amazonaws.com"
	path="/experiments/cloudsimex/cloudsimex-experiments/multi-cloud/stat"
    fi

    if [ $1 == "openstack" ]
    then
        sshTerm="iaas@iaas.cis.unimelb.edu.au"
	path="/home/iaas/nik/stat"
    fi
fi

echo 
command="scp $sshTerm:$path/*.zip $localPath"
echo $command
echo 
echo 
echo 
scp $sshTerm:$path/*.zip $localPath

#for f in `ssh $sshTerm "ls $path | egrep \".zip$\"" `
#do
#    echo "Copying:" $f " ..."
#    scp $sshTerm:"\"$path/$f\"" $localPath
#    echo 
#    echo
#done


