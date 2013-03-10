source('parseRubis.R')
source('parseSar.R')
source('util.R')

plotDelayComparison <- function(forWorkload = "All", baseSize = 100, file = NA) {
  
  baseLineFile <- paste0(subDir, "/sessions_", baseSize, ".txt")
  
  if(forWorkload[1] == "All") {
    forWorkload <- sapply (getFiles(sessionPattern), getNumbersInNames )
  } 
  
  sessionFiles <-sapply(forWorkload, function(w) {paste0(subDir, "/", "sessions_",w, ".txt")})
  simSessionFiles <- sapply(forWorkload, function(w) {paste0(subDir, "/", "simulation_sessions_", w, ".csv")})
  
  baseLineFrame <- read.csv(baseLineFile)

  data <- c()
  i = 0
  for(sessFile in sessionFiles) {
    i <- i + 1
    if(sessFile != baseLineFile) {
      simFile <- simSessionFiles[i]
      if(getNumbersInNames(sessFile) <= baseLineSize || !file.exists(simFile) || !file.exists(sessFile)) {
        next
      }
      
      sessFrame <- read.csv(sessFile)
      simFrame <- read.csv(simFile, sep = ";")
      
      simDelay <- mean(simFrame$Delay)
      sessFrame$simDelay <- sapply(1:nrow(sessFrame), function(x) {simDelay} )
      
      sessFrame$delay <- sessFrame$duration - mean(baseLineFrame$duration)
      sessFrame$factor <- getNumbersInNames(sessFile)
      
      data <- if (length(data) == 0) {
        sessFrame
      } else {
        rbind(data, sessFrame)
      }
    }
  }

  openGraphsDevice(file)
  fullScreen()
  boxplot(data$delay ~ data$factor, ylab = "Session Delay in Seconds", xlab="Number of Sessions")
  boxplot(data$simDelay ~ data$factor, border = "red", lwd = 1, add = TRUE, boxwex=0.9)
  resetMar()
  closeDevice(file)
}

compareUtilisation <- function(forWorkload = "All", property="percentCPU", type, vmId) {
  if(forWorkload[1] == "All") {
    forWorkload <- sapply (getFiles(sessionPattern), getNumbersInNames )
  } 
  
  sessionFiles <-sapply(forWorkload, function(w) {paste0(subDir, "/", "sessions_",w, ".txt")})
  simSessionFiles <- sapply(forWorkload, function(w) {paste0(subDir, "/", "simulation_sessions_", w, ".csv")}) 
  
  baseLineFile <- paste0(subDir, "/", type, "_server_1")
  baseLineFrame <- parseSar(baseLineFile)
  
  i <- 0
  for(w in forWorkload) {
    i <- i + 1
    
    sarFile <- paste0(subDir, "/", type, "_server_", w)
    simFile <- simSessionFiles[i]
    
    if(!file.exists(simFile) || !file.exists(sarFile)) {
      next
    }

    sarFrame <- prepareSarFrame(parseSar(sarFile), baseLineFrame)
    sarFrame <- renameSarColToSim(sarFrame)
    simFrame <- parseSimulationPerformanceResults(w, vmId, 1, mean)
    
    len <- min(nrow(sarFrame), nrow(simFrame))
    
    print(paste("======================> Wilcox for ", w, " sessions <====================="))
    vector1 <- sarFrame[,property][1:len]
    vector2 <- simFrame[,property][1:len]
    print(wilcox.test(vector1[1:len], vector2[1:len], paired=TRUE))
    print("")
  }
  
}

plotComparisonSimExecPerfBulk <- function(forWorkload, type, vmId, property="percentCPU", step = 10, stepFunc = mean, filePattern, maxY=NA) {
  
  for(w in forWorkload) {
    fileName <- paste0(subDir, "/", filePattern, "_", property, "_",  w, ".pdf" )
    plotComparisonSimExecPerf(w, type, vmId, property, step, stepFunc=stepFunc, file = fileName, maxY=maxY)
  }
  
}

plotComparisonSimExecPerf <- function(forWorkload, type, vmId, property="percentCPU", step = 5, stepFunc = stepFuncDefault, maxY = NA, preparePlot = T, file = NA){
  propertiesToNames <- c("percentCPU" = "CPU", "percentRAM" = "RAM", "percentIO" = "Disk IO")
  
  baseLineFile <- paste0(subDir, "/", type, "_server_", baseLineSize)
  baseLineFrame <- parseSar(baseLineFile)
  
  sarFile <- paste0(subDir, "/", type, "_server_", forWorkload)
  sarFrame <- prepareSarFrame(parseSar(sarFile), baseLineFrame)
  
  sarFrame <- renameSarColToSim(sarFrame)
  simFrame <- parseSimulationPerformanceResults(forWorkload, vmId, step, stepFunc)
  
  if(property == "percentRAM") {
    baseLineFrame$prcActive <- 100 * as.numeric(baseLineFrame$kbactive) /
      (as.numeric(baseLineFrame$kbmemused) + as.numeric(baseLineFrame$kbmemfree))
    meanActive <- mean(baseLineFrame$prcActive) 
    sarFrame[,property] <- sarFrame[,property] + meanActive
    simFrame[,property] <- simFrame[,property] + meanActive
  }
  
  minTime <- min(min(sarFrame$time),  min(simFrame$time))
  maxTime <- min(getSessionLastTimeByWorkload(forWorkload),  max(simFrame$time))
  
  msrStep <- simFrame$time[2] - simFrame$time[1]
  step <- step / msrStep
  
  minProp <- 0
  maxProp <- if (is.na(maxY)) 100 else maxY

  openGraphsDevice(file)
  fullScreen(hasTitle=T)
  
  if (preparePlot) {
    yLable = paste0("% ", propertiesToNames[property], " utilisation")
    title = paste0(yLable, " for ", forWorkload, " sessions") 
    
    plot(minProp, minTime, ylim=c(minProp, maxProp), xlim=c(minTime, maxTime), type = "n", main = title,
       xlab = "Time in seconds",
       ylab = yLable)
  }
  
  lines(simFrame[,property]~simFrame$time,  type="l", lwd=2, lty = 1, pch = 18, col="red")
  lines(sarFrame[,property]~sarFrame$time,  type="l", lty = 2, pch = 19, col="black")
  
  resetMar()
  closeDevice(file)
  
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
