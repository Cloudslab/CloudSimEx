#!/bin/bash


cd ./results
data=(50 1 30 2 \
  50  1 30 1 \
  150 1 30 5 \  
  200 1 20 5 \
  200 1 30 10 )

step=4
len=${#data[@]}
i=0
start=$i
end=$i+$step

while [  $i -lt $len ]
do
    let start=$i
    let end=$i+$step
    echo "== === == == == == == == == == == == == == == == == == =="
    echo Data: ${data[@]:$start:$step}
    for j in 1 .. 10
    do 
	echo 
    done
    
    
    Rscript MultiCloudFrameworkEvaluation.R `echo ${data[@]:$start:$end}`
    let i=i+$step
done


