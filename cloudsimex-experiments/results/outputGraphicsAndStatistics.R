# Outputs all graphics. Expects to be provided with a command line arguement
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
source('parseSimulation.R')

resetPar()

workloads <- c(50, 100, 200, 300, 400, 500, 600)

##plotDelayComparison(file = paste0(subDir, "/delays_boxplots.pdf"), baseSize=100, forWorkload=workloads)
##plotComparison(type="db", forWorkload=c(100,1), plotLegend=T, useColors=F, file = paste0(subDir, "/1-100SessionsBaseline.pdf"), maxY=30, hasTitle=F, useLineTypes=c(3, 1))

plotComparison(type="db", forWorkload=c(100,1), plotLegend=T, useColors=F, property = "%tps",
               file = paste0(subDir, "/1-100_IO_SessionsBaseline.pdf"), maxY=30, hasTitle=F, useLineTypes=c(3, 1))

plotComparisonOfProperties(workload = 100, step = 120, file = paste0(subDir, "/100-CMPUtilisation.pdf"), maxY = 30)

plotComparisonOfPropsBulk(c(300,600), step = 90)


step <- 50
namePattern <- "cmp"
asPattern <- "as_cmp"
dbPattern <- "db_cmp"

plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "db", vmId = 1, filePattern = dbPattern, step = step)
plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "web", vmId = 2, filePattern = asPattern, step = step)

##plotComparisonSimExecPerfBulk(forWorkload=c(200, 600), type="db", vmId=1, filePattern=dbPattern, step = 10, layoutMatrix=matrix(c(1, 2), 1, 2, byrow = TRUE))
##plotComparisonSimExecPerfBulk(forWorkload=c(200, 700), type="db", vmId=1, filePattern=dbPattern, step = 10, layoutMatrix=matrix(c(1, 2), 1, 2, byrow = TRUE))

plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "db", property="percentRAM", vmId = 1, filePattern = dbPattern, step = step, maxY = 100)
plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "web", property="percentRAM", vmId = 2, filePattern = asPattern, step = step, maxY = 100)

plotComparisonSimExecPerfBulk(forWorkload=workloads, type = "db", property="percentIO", vmId = 1, filePattern = dbPattern, step = step, maxY = 100)

plotComparisonSimExecPerfBulk(forWorkload=c(50, 300, 600), type = "db", property="percentIO", vmId = 1, filePattern = dbPattern, step = step, maxY = 100, maxX = 1400,
                              layoutMatrix=matrix(c(1, 2, 3, 4), 1, 4, byrow = TRUE), layoutWidths=c(2.3, 4, 3.1, 3.1), layoutHeigths=c(5, 5, 5, 5), lwdVal = 1)

outFile <- paste0(subDir, "/CPU_DB_SRV.test.txt")
compareUtilisation(forWorkload=workloads[-2], type = "db", vmId = 1, file = outFile)

outFile <- paste0(subDir, "/CPU_AS_SRV.test.txt")
compareUtilisation(forWorkload=workloads[-2], type = "web", vmId = 2, file = outFile)

outFile <- paste0(subDir, "/RAM_DB_SRV.test.txt")
compareUtilisation(forWorkload=workloads[-2], property="percentRAM", type = "db", vmId = 1, file = outFile)

outFile <- paste0(subDir, "/RAM_AS_SRV.test.txt")
compareUtilisation(forWorkload=workloads[-2], property="percentRAM", type = "web", vmId = 2, file = outFile)

outFile <- paste0(subDir, "/IO_DB_SRV.test.txt")
compareUtilisation(forWorkload=workloads[-2], property="percentIO", type = "db", vmId = 1, file = outFile)

sink()

print("Diagrams and wilcox results have been generated")
