source('parseRubis.R')
source('parseSar.R')

plotComparisonSimulationexecution <- function(forWorkload, type, vmId, property="percentCPU", step = 5, stepFunc = stepFuncDefault, maxY = NA){
  baseLineFile <- paste0(subDir, "/", type, "_server_0")
  baseLineFrame <- parseSar(baseLineFile)
  
  sarFile <- paste0(subDir, "/", type, "_server_", forWorkload)
  sarFrame <- prepareSarFrame(parseSar(sarFile), baseLineFrame)
  
  sarFrame <- renameSarColToSim(sarFrame)
  simFrame <- parseSimulationPerformanceResults(forWorkload, vmId, step, stepFunc)
  
  minTime <- min(min(sarFrame$time),  min(simFrame$time))
  maxTime <- max(getSessionLastTimeByWorkload(forWorkload),  max(simFrame$time))
  
  msrStep <- simFrame$time[2] - simFrame$time[1]
  step <- step / msrStep
  
  minProp <- 0
  maxProp <- if (is.na(maxY)) 100 else maxY
  
  plot(minProp, minTime, ylim=c(minProp, maxProp), xlim=c(minTime, maxTime), type = "n", main = property,
       xlab = "Time in seconds",
       ylab = property)
  
  lines(simFrame[,property]~simFrame$time,  type="l", lty = 1, pch = 18, col="red")
  lines(sarFrame[,property]~sarFrame$time,  type="l", lty = 2, pch = 19, col="black")
  
  print("Summary of simulation performances")
  print(summary(simFrame[,property]))
  print("Summary of execution performances")
  print(summary(sarFrame[,property]))
}

parseSimulationPerformanceResults <- function(forWorkload, vmId, step = 5, stepFunc = stepFuncDefault) {
  simFile = paste0(subDir, "/", "performance_sessions_", forWorkload, ".csv")
  simDf <- read.csv(simFile, sep=";")
  
  simPerf <-simDf[simDf$vmId == vmId, ]

  simPerf$percentCPU = averageSteps(simPerf$percentCPU, step, stepFunc)
  simPerf$percentRAM = averageSteps(simPerf$percentRAM, step, stepFunc)
  
  if("percentIO" %in% names(simPerf)) {
    simPerf$percentIO = averageSteps(simPerf$percentIO, step, stepFunc)
  }
  
  #Get only the data for the seconds(steps) we need
  simPerf <- simPerf[seq(1, nrow(simPerf), step),]
  simPerf <- simPerf[complete.cases(simPerf),]
  
  simPerf
}

renameSarColToSim <- function(sarDf) {
  newNamesVector <-
    c("Time"="time", "%CPUUtil"="percentCPU", "%ActiveMem"="percentRAM", "%SessionMem"="USED_RAM", "%tps"="percentIO")
  names(sarDf) <- sapply(names(sarDf), function(n) {if(is.na(newNamesVector[n])) n else newNamesVector[n] })
  sarDf  
}
