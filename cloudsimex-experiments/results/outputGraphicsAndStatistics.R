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
plotComparison(type="db", forWorkload=c(100,1), plotLegend=T, useColors=F, file = paste0(subDir, "/1-100SessionsBaseline.pdf"))


workloads <- seq(100, 700, 100)
step <- 20
namePattern <- "cmp"
asPattern <- "as_cmp"
dbPattern <- "db_cmp"

plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "db", vmId = 1, filePattern = dbPattern, step = step)
plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "web", vmId = 2, filePattern = asPattern, step = step)

plotComparisonSimExecPerfBulk(forWorkload=c(200, 600), type="db", vmId=1, filePattern=dbPattern, step = 10, layoutMatrix=matrix(c(1, 2), 1, 2, byrow = TRUE))
plotComparisonSimExecPerfBulk(forWorkload=c(200, 700), type="db", vmId=1, filePattern=dbPattern, step = 10, layoutMatrix=matrix(c(1, 2), 1, 2, byrow = TRUE))

plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "db", property="percentRAM", vmId = 1, filePattern = dbPattern, step = 15, maxY = 100)
plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "web", property="percentRAM", vmId = 2, filePattern = asPattern, step = 15, maxY = 100)

plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "db", property="percentIO", vmId = 1, filePattern = dbPattern, step = step, maxY = 10)


outFile <- paste0(subDir, "/CPU_DB_SRV.test.txt")
compareUtilisation(forWorkload=workloads[-1], type = "db", vmId = 1, file = outFile)

outFile <- paste0(subDir, "/CPU_AS_SRV.test.txt")
compareUtilisation(forWorkload=workloads[-1], type = "web", vmId = 2, file = outFile)

outFile <- paste0(subDir, "/RAM_DB_SRV.test.txt")
compareUtilisation(forWorkload=workloads[-1], property="percentRAM", type = "db", vmId = 1, file = outFile)

outFile <- paste0(subDir, "/RAM_AS_SRV.test.txt")
compareUtilisation(forWorkload=workloads[-1], property="percentRAM", type = "web", vmId = 2, file = outFile)

outFile <- paste0(subDir, "/IO_DB_SRV.test.txt")
compareUtilisation(forWorkload=workloads[-1], property="percentIO", type = "db", vmId = 1, file = outFile)

sink()

print("Diagrams and wilcox results have been generated")
