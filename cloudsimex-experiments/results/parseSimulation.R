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
  boxplot(data$simDelay ~ data$factor, border = "red", lwd = 1, add = TRUE, boxwex=0.9, boxlty=1, lty = 2)
  resetMar()
  closeDevice(file)
}

compareExp2 <- function(sarFile, simFile, vmId, maxTPSOperations, file=NA, property="percentCPU", type, activeMem=NA) {
  sarFrame <- prepareSarFrame0(parseSar(paste0(subDir, "/", sarFile)), maxTPSOps=maxTPSOperations, type=type, activeMem=activeMem)
  sarFrame <- renameSarColToSim(sarFrame)
  simFrame <- parseSimulationPerformanceResults0(paste0(subDir, "/", simFile), vmId, 60, mean)
  
  len <- min(nrow(sarFrame), nrow(simFrame))
  
  print(paste("======================> ", sarFile, simFile, property, vmId, "  <====================="))
  
  sarVec <- sarFrame[,property][1:len]
  simVec <- simFrame[,property][1:len]
  diff <- simVec - sarVec
  
  print(paste("Ssamples length", len))
  print("First 50 elements from execution:")
  print(sarVec[1:50])
  
  print("First 50 elements from simulation:")
  print(simVec[1:50])
  
  print("First 50 elements from the difference:")
  print(diff[1:50])
  
  t <- wilcox.test(simVec[1:len], sarVec[1:len], paired=TRUE, conf.int=T) 
  if (!is.na(file)) {
    sink(file, append=T, split=FALSE)
    s1 <- sprintf("%s 95%s CI (%.2f, %.2f)", property, "%", t$conf.int[1], t$conf.int[2])
    s2 <- sprintf("; %.2f  ", t$estimate)
    resString <- sprintf("%-50s %-50s", s1, s2)
    print(resString)
    sink()
  }
  
  print(t)
  
}

compareUtilisation <- function(forWorkload = "All", property="percentCPU", type, vmId, file=NA) {
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
    
    print(paste("======================> ", w, " sessions <====================="))
    sink()
    
    sarVec <- sarFrame[,property][1:len]
    simVec <- simFrame[,property][1:len]
    diff <- simVec - sarVec
    
    print(paste("Ssamples length", len))
    print("First 50 elements from execution:")
    print(sarVec[1:50])
    
    print("First 50 elements from simulation:")
    print(simVec[1:50])
    
    print("First 50 elements from the difference:")
    print(diff[1:50])
    
    t <- wilcox.test(simVec[1:len], sarVec[1:len], paired=TRUE, conf.int=T) 
    if (!is.na(file)) {
      sink(file, append=T, split=FALSE)
      s1 <- sprintf("%d sessions  95%s CI (%.2f, %.2f)", w, "%", t$conf.int[1], t$conf.int[2])
      s2 <- sprintf("; %.2f  ", t$estimate)
      resString <- sprintf("%-50s %-50s", s1, s2)
      print(resString)
      sink()
    }
    
    print(t)
    
    print("")
    print("Testing with the diff - should be the same result...")
    print(wilcox.test(diff, conf.int=T))
  }
  
}

plotComparisonSimExecPerfBulk <- function(forWorkload, type, vmId, property="percentCPU", step = 10, stepFunc = mean, filePattern, maxY=NA, layoutMatrix=NA) {
  concatNames <- paste(forWorkload, collapse = '-')
  commonFile <- if (is.na(filePattern)) NA else paste0(subDir, "/", filePattern, "_", property, "_",  concatNames, ".pdf" )
  if (!is.na(layoutMatrix)) {
    openGraphsDevice(commonFile)
    layout(layoutMatrix)
  }
  
  for(w in forWorkload) {
    fileName <- if (!is.na(layoutMatrix) || is.na(filePattern)) NA else paste0(subDir, "/", filePattern, "_", property, "_",  w, ".pdf" )
    #if(is.na(fileName) || file.exists(fileName)) {
      plotComparisonSimExecPerf(w, type=type, vmId=vmId, property=property, step=step, stepFunc=stepFunc, file = fileName, maxY=maxY)
    #}
  }
  
  if (!is.na(layoutMatrix)) {
    closeDevice(commonFile)
  }
}

plotComparisonSimExecPerf <- function(forWorkload, type, vmId, property="percentCPU", step = 5, stepFunc = stepFuncDefault, maxY = NA, preparePlot = T,
                                      file = NA, titleFlag=F, plotLegend=T){

  sarFile <- paste0(subDir, "/", type, "_server_", forWorkload)
  simFile = paste0(subDir, "/", "performance_sessions_", forWorkload, ".csv")
  
  if(file.exists(sarFile) && file.exists(simFile)){
    propertiesToNames <- c("percentCPU" = "CPU", "percentRAM" = "RAM", "percentIO" = "Disk IO")
    
    baseLineFile <- paste0(subDir, "/", type, "_server_", baseLineSize)
    baseLineFrame <- parseSar(baseLineFile)
    
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
    fullScreen(hasTitle=titleFlag)
    
    if (preparePlot) {
      yLable = paste0("% ", propertiesToNames[property], " utilisation")
      title = if(titleFlag) paste0(forWorkload, " sessions") else " "
      
      plot(minProp, minTime, ylim=c(minProp, maxProp), xlim=c(minTime, maxTime), type = "n", main = title,
         xlab = "Time in seconds",
         ylab = yLable)
    }
    
    lines(simFrame[,property]~simFrame$time,  type="l", lwd=3, lty = 1, pch = 18, col="red")
    lines(sarFrame[,property]~sarFrame$time,  type="l", lty = 3, pch = 19, col="black")
    
    if (plotLegend) {
      legend(0, 100, c("Simulation", "Execution"),
             col = c("red", "black"), lty = c(1, 2), cex=0.7)
    }
    
    resetMar()
    closeDevice(file)
    
    print("Summary of simulation performances")
    print(summary(simFrame[,property]))
    print("Summary of execution performances")
    print(summary(sarFrame[,property]))
  }
}


plotExp2SimPerf <- function(simFile, vmIds, property="percentCPU", step = 1, stepFunc = mean, maxY = NA,
                            file = NA, title="AS CPU utilisation in DC1 & DC2 at simulation time") {
  propertiesToNames <- c("percentCPU" = "CPU", "percentRAM" = "RAM", "percentIO" = "Disk IO")

  minTime <- 0
  maxTime <- 24 * 60 * 60

  minProp <- 0
  maxProp <- if (is.na(maxY)) 100 else maxY
  
  yLable = paste0("% ", propertiesToNames[property], " utilisation")
  
  openGraphsDevice(file)
  fullScreen(hasTitle=F)
  
  plot(minProp, minTime, ylim=c(minProp, maxProp), xlim=c(minTime, maxTime), type = "n", main = "",
       xlab = "Time after simulation's start",
       ylab = yLable,
       xaxt='n',
       cex.main=0.9)
  xDelta <- 3600 * 3
  xpos <- seq(0, maxTime, by=xDelta)
  print(maxTime)
  axis(side=1, at=xpos, labels= sapply(xpos, function(x){toDateString(x)}))
  
  ltys = c(1, 2)
  colors = c("black", "red")
  i = 1
  for (vmId in vmIds) {
    simFrame <- parseSimulationPerformanceResults0(simFile, vmId, step, stepFunc)
    
    if(property == "percentRAM") {
      baseLineFrame$prcActive <- 100 * as.numeric(baseLineFrame$kbactive) /
        (as.numeric(baseLineFrame$kbmemused) + as.numeric(baseLineFrame$kbmemfree))
      meanActive <- mean(baseLineFrame$prcActive) 
      simFrame[,property] <- simFrame[,property] + meanActive
    }
    
    #msrStep <- simFrame$time[2] - simFrame$time[1]
    #step <- step / msrStep
    
    lines(simFrame[,property]~simFrame$time,  type="l", lwd=1, lty = ltys[i], pch = 18, col=colors[i])
    i <- i + 1
  }
  
  legend(0, maxY, c("DC1", "DC2"),
         col = colors, lty = ltys, cex=0.7)
  
  resetMar()
  closeDevice(file)
  
  #print("Summary of simulation performances")
  #print(summary(simFrame[,property]))
}

parseSimulationPerformanceResults <- function(forWorkload, vmId, step = 5, stepFunc = stepFuncDefault) {
  simFile = paste0(subDir, "/", "performance_sessions_", forWorkload, ".csv")
  parseSimulationPerformanceResults0(simFile, vmId, step, stepFunc)
}

parseSimulationPerformanceResults0 <- function(simFile, vmId, step = 5, stepFunc = stepFuncDefault) {
  simDf <- read.csv(simFile, sep=";")
  
  simPerf <-simDf[simDf$vmId == vmId, ]
  
  msrStep <- simPerf$time[2] - simPerf$time[1]
  actualStep <- step / msrStep
  
  simPerf$percentCPU = averageSteps(simPerf$percentCPU, actualStep, stepFunc)
  simPerf$percentRAM = averageSteps(simPerf$percentRAM, actualStep, stepFunc)
  
  if("percentIO" %in% names(simPerf)) {
    simPerf$percentIO = averageSteps(simPerf$percentIO, actualStep, stepFunc)
  }
  
  #Get only the data for the seconds(steps) we need
  simPerf <- simPerf[seq(1, nrow(simPerf), actualStep),]
  simPerf <- simPerf[complete.cases(simPerf),]
  
  simPerf
}

renameSarColToSim <- function(sarDf) {
  newNamesVector <-
    c("Time"="time", "%CPUUtil"="percentCPU", "%ActiveMem"="percentRAM", "%SessionMem"="USED_RAM", "%tps"="percentIO")
  names(sarDf) <- sapply(names(sarDf), function(n) {if(is.na(newNamesVector[n])) n else newNamesVector[n] })
  sarDf  
}
