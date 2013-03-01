# Runs the analysis. Expects to be provided with a command line arguement
# for the working directory - e.g. $RBIN --file=./run.R --args $1

args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) >= 1){
  setwd(args[1])
}

source('parseRubis.R')
source('parseSar.R')

prepareSessionData()
