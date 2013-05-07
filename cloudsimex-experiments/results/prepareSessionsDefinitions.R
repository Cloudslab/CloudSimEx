
# Runs the analysis. Expects to be provided with a command line arguement
# for the working directory - e.g. $RBIN --file=./run.R --args $1

args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) >= 1){
  setwd(args[1])
  subDir<-"tmp"
}

source('util.R')
source('parseRubis.R')
source('parseSar.R')

prepareSessionData(stepFunc=median, size=100, step=60)
