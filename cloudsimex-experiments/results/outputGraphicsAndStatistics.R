# Outputs all graphics. Expects to be provided with a command line arguement
# for the working directory - e.g. $RBIN --file=./run.R --args $1

args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) >= 1){
  setwd(args[1])
}

source('util.R')
source('parseRubis.R')
source('parseSar.R')
source('parseSimulation.R')

resetPar()

plotDelayComparison(file = paste0(subDir, "/delays_boxplots.pdf") )

workloads <- seq(100, 700, 100)
step <- 20
namePattern <- "cmp"

plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "db", vmId = 1, filePattern = namePattern, step = step)
plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "web", vmId = 2, filePattern = namePattern, step = step)

plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "db", property="percentRAM", vmId = 1, filePattern = namePattern, step = 5, maxY = 25)
plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "web", property="percentRAM", vmId = 2, filePattern = namePattern, step = 5, maxY = 25)

plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "db", property="percentIO", vmId = 1, filePattern = namePattern, step = step, maxY = 10)


sink(paste0(subDir, "/CPU_DB_SRV.txt"), append=FALSE, split=FALSE)
compareUtilisation(forWorkload=workloads, type = "db", vmId = 1)

sink(paste0(subDir, "/CPU_AS_SRV.txt"), append=FALSE, split=FALSE)
compareUtilisation(forWorkload=workloads, type = "web", vmId = 2)

sink(paste0(subDir, "/RAM_DB_SRV.txt"), append=FALSE, split=FALSE)
compareUtilisation(forWorkload=workloads, property="percentRAM", type = "db", vmId = 1)

sink(paste0(subDir, "/RAM_AS_SRV.txt"), append=FALSE, split=FALSE)
compareUtilisation(forWorkload=workloads, property="percentRAM", type = "web", vmId = 2)

sink(paste0(subDir, "/IO_DB_SRV.txt"), append=FALSE, split=FALSE)
compareUtilisation(forWorkload=workloads, property="percentIO", type = "db", vmId = 1)

sink()

print("Done")
