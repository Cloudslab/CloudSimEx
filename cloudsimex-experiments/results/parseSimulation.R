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

plotComparisonSimExecPerfBulk <- function(forWorkload, type, vmId, property="percentCPU", step = 10, stepFunc = mean, filePattern,
                                          maxY=NA, layoutMatrix=NA, layoutWidths = c(), layoutHeigths = c(), maxX=NA, lwdVal = 3) {
  concatNames <- paste(forWorkload, collapse = '-')
  commonFile <- if (is.na(filePattern)) NA else paste0(subDir, "/", filePattern, "_", property, "_",  concatNames, ".pdf" )
  if (!is.na(layoutMatrix)) {
    openGraphsDevice(commonFile)
    layout(layoutMatrix, widths = layoutWidths, heights = layoutHeigths, respect=T)
    
    fullScreen(hasTitle = T, keepLeftMargin = F, minBorder = 0)
    plot(0, 100, ylim=c(0, 200), xlim=c(0, 100), type = 'n', axes = FALSE, xlab = '', ylab = '', main = "")
    legend(0, 200, c("Simulation", "Execution"),
           col = c("red", "black"), lty = c(1, 2), cex=0.7)
  }
  
  plotLableOnY = T
  plotLegendFlag = is.na(layoutMatrix)
  titleFlag = !is.na(layoutMatrix)
  xLableIdx = trunc(length(forWorkload) / 2)
  i = 0
  for(w in forWorkload) {
    fileName <- if (!is.na(layoutMatrix) || is.na(filePattern)) NA else paste0(subDir, "/", filePattern, "_", property, "_",  w, ".pdf" )
    #if(is.na(fileName) || file.exists(fileName)) {
    
      plotComparisonSimExecPerf(w, type=type, vmId=vmId, property=property, step=step, stepFunc=stepFunc, file = fileName, maxY=maxY,
                                plotXLable = (xLableIdx == i), plotYLable = plotLableOnY, plotLegend = plotLegendFlag,
                                titleFlag = titleFlag, yaxtFlag = plotLableOnY, maxX=1400, lwdVal = lwdVal)
      plotLableOnY = if (!is.na(layoutMatrix)) F else plotLableOnY
      i <- i + 1
    #}
  }
  
  
  if (!is.na(layoutMatrix)) {
    closeDevice(commonFile)
  }
}

plotComparisonSimExecPerf <- function(forWorkload, type, vmId, property="percentCPU", step = 5, stepFunc = stepFuncDefault, maxY = NA, preparePlot = T,
                                      file = NA, titleFlag=F, plotLegend=T, plotYLable=T, plotXLable=T, yaxtFlag=T, maxX=NA, lwdVal = 3) {

  sarFile <- paste0(subDir, "/", type, "_server_", forWorkload)
  simFile = paste0(subDir, "/", "performance_sessions_", forWorkload, ".csv")
  
  if(file.exists(sarFile) && file.exists(simFile)){
    propertiesToNames <- c("percentCPU" = "CPU", "percentRAM" = "RAM", "percentIO" = "Disk ")
    
    baseLineFile <- paste0(subDir, "/", type, "_server_", baseLineSize)
    baseLineFrame <- parseSar(baseLineFile)
    
    sarFrame <- prepareSarFrame(parseSar(sarFile), baseLineFrame)
    sarFrame <- renameSarColToSim(sarFrame)
    
    ##### ##### #####
    if("percentIO" %in% names(sarFrame)) {
      msrStep <- sarFrame$time[2] - sarFrame$time[1]
      actualStep <- 3 #step / (msrStep * 5)
    
      sarFrame$percentIO = sapply(sarFrame$percentIO, function(x) {if (x > 100) 100 else x  } )
      sarFrame$percentIO = averageSteps(sarFrame$percentIO, actualStep, stepFunc)
    }
    ##### ##### #####
    
    simFrame <- parseSimulationPerformanceResults(forWorkload, vmId, step, stepFunc)
    
    if(property == "percentRAM") {
      baseLineFrame$prcActive <- 100 * as.numeric(baseLineFrame$kbactive) /
        (as.numeric(baseLineFrame$kbmemused) + as.numeric(baseLineFrame$kbmemfree))
      meanActive <- mean(baseLineFrame$prcActive) 
      sarFrame[,property] <- sarFrame[,property] + meanActive
      simFrame[,property] <- simFrame[,property] + meanActive
    }
    
    minTime <- min(min(sarFrame$time),  min(simFrame$time))
    maxTime <- if(is.na(maxX))  min(getSessionLastTimeByWorkload(forWorkload),  max(simFrame$time)) else maxX
    
    msrStep <- simFrame$time[2] - simFrame$time[1]
    step <- step / msrStep
    
    minProp <- 0
    maxProp <- if (is.na(maxY)) 100 else maxY
  
    openGraphsDevice(file)
    fullScreen(hasTitle=titleFlag, keepLeftMargin = yaxtFlag, minBorder = 0.5)
    
    if (preparePlot) {
      yLable = if(plotYLable) paste0("% ", propertiesToNames[property], " utilisation") else ""
      xLable = if(plotXLable) "Time in seconds" else "" 
      title = if(titleFlag) paste0(forWorkload, " sessions") else " "
      
      if (yaxtFlag) {
        plot(minProp, minTime, ylim=c(minProp, maxProp), xlim=c(minTime, maxTime), type = "n", main = title,
          cex.main=0.9,
          xlab = xLable,
          ylab = yLable)
      } else {
        plot(minProp, minTime, ylim=c(minProp, maxProp), xlim=c(minTime, maxTime), type = "n", main = title,
             cex.main=0.9,
             xlab = xLable,
             ylab = yLable, yaxt = 'n')
      }
    }
    
    lines(simFrame[,property]~simFrame$time,  type="l", lwd=lwdVal, lty = 1, pch = 18, col="red")
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


plotWorkload <- function(file = NA, wldf=1, legendNames=c("DC1", "DC2"), size=NA) {
  dc1Freqs <- createFreqsDF(wldf=wldf)
  dc2Freqs <- createFreqsDF(offset = 12, wldf=wldf)
  #print(dc1Freqs)
  #print(dc2Freqs)
  
  fullScreen(hasTitle = T, keepLeftMargin = F, minBorder = 0)
  par(mgp = c(0, 0.5, 0.5))
  
  
  if(is.na(size)) {
    openGraphsDevice(file)
  } else {
    openSizedGraphsDevice(file, width=size[1], height=size[2])
  }
  
  layoutMatrix <- matrix(c(1), 1, 1, byrow = TRUE)
  layoutWidths=c(4.5)
  layoutHeigths=c(2.5)
  layout(layoutMatrix, widths = layoutWidths, heights = layoutHeigths, respect=T)
  
  fullScreen(hasTitle=F)
  legendSpace <- if(wldf > 1) wldf*15 else 15
  plot(0, 0, ylim=c(0, legendSpace + max(max(dc1Freqs$freq), max(dc2Freqs$freq))), xlim=c(0, 24), type = "n", main = NA,
       xlab = "Time after experiment's start",
       ylab = "Average Session Arrivals",
       xaxt='n',
       cex.main=0.9)
  
  lines(dc1Freqs$freq~dc1Freqs$time,  type="l", lwd=1, lty = 1, pch = 18, col="black")
  lines(dc2Freqs$freq~dc2Freqs$time,  type="l", lwd=1, lty = 2, pch = 18, col="red")
  
  xDelta <- 3
  xpos <- seq(0, 24, by=xDelta)
  labels <- sapply(xpos, function(x){paste0(x, 'h')})
  axis(side=1, at=xpos, labels=labels)
  
  legend(0*wldf, 100*wldf + legendSpace, legendNames,
         col = c("black","red"), lty = c(1,2), cex=0.7)
  closeDevice(file)
}

createFreqsDF <- function (offset = 0, wldf=1) {
  freqs <- data.frame(time = c( 0,  6,  6,  7,  7,  10,  10,  14,  14,  17,  17,  18,  18,  24),
                      freq = c(10, 10, 30, 30, 50,  50, 100, 100,  50,  50,  30,  30,  10,  10) * wldf)
  freqs$time <- sapply(freqs$time, function(x) {if(offset == 0) x else (x + offset) %% 24})
  freqs <- freqs[order(freqs$time),]
  
  delta = 0.01
  for(i in 1:(nrow(freqs) - 1)) {
    #print(freqs[i, 1] == freqs[i+1, 1])
    #print(freqs[i, 1])
    #print(freqs[i+1, 1])
    if (freqs[i, 1] == freqs[i+1, 1]) {
      freqs[i, 1] = freqs[i, 1] - delta
      freqs[i+1, 1] = freqs[i+1, 1] + delta
    } 
  }
  
  if(!0 %in% freqs$time) {
    freqs[nrow(freqs) + 1, ] = c(0, freqs[1, 2])
  }
  if(! 24 %in% freqs$time) {
    freqs[nrow(freqs) + 1, ] = c(24, freqs[nrow(freqs), 2])
  }
  
  freqs[order(freqs$time),]
}



plotExp2SimPerfBulk <- function() {
  commonFile <- paste0(subDir, "/UtilisationExp2Simulation.pdf")
  layoutMatrix <- matrix(c(1, 2, 3, 4), 1, 4, byrow = TRUE)
  layoutWidths=c(1, 4.3, 3.5, 3.5)
  layoutHeigths=c(5.2, 5.2, 5.2, 5.2)
  
  openGraphsDevice(commonFile)
  
  layout(layoutMatrix, widths = layoutWidths, heights = layoutHeigths, respect=T)
  
  fullScreen(hasTitle = T, keepLeftMargin = F, minBorder = 0)
  plot(0, 100, ylim=c(0, 200), xlim=c(0, 100), type = 'n', axes = FALSE, xlab = '', ylab = '', main = "")
  legend(0, 200, c("DC1", "DC2"), col = c("black", "red"), lty = c(1, 5), cex=0.7)
  
  step = 300
  maxY = 10
  simFile = paste0(subDir, "/performance_sessions_DC1_2.csv")
  plotExp2SimPerf(simFile=simFile, vmIds=c(2,4), property="percentCPU", yLable = "% Utilisation", keepYLable = T,
                  step = step, stepFunc = mean, maxY = maxY, title="AS Server, CPU", plotXLable = F, plotLegend = F)
  plotExp2SimPerf(simFile=simFile, vmIds=c(1,3), property="percentCPU", keepYLable = F,
                  step = step, stepFunc = mean, maxY = maxY, title="DB Server, CPU", plotXLable = T, plotLegend = F, yLable = NA)
  plotExp2SimPerf(simFile=simFile, vmIds=c(1,3), property="percentIO", keepYLable = F,
                  step = step, stepFunc = mean, maxY = maxY, title="DB Server, Disk", plotXLable = F, plotLegend = F, yLable = NA)
  
  closeDevice(commonFile)
}

plotExp2SimPerf <- function(simFile, vmIds, property="percentCPU", step = 1, stepFunc = mean, maxY = NA, keepYLable = T, yLable = NA,
                            file = NA, title="AS CPU utilisation in DC1 & DC2 at simulation time", plotXLable = F, plotLegend = T) {
  propertiesToNames <- c("percentCPU" = "CPU", "percentRAM" = "RAM", "percentIO" = "Disk IO")

  minTime <- 0
  maxTime <- 24 * 60 * 60

  minProp <- 0
  maxProp <- if (is.na(maxY)) 100 else maxY
  
  yLable = if (is.na(yLable)) paste0("% ", propertiesToNames[property], " utilisation") else yLable
  
  openGraphsDevice(file)
  fullScreen(hasTitle=!is.na(title), minBorder=0.5, keepLeftMargin=keepYLable)
  
  if (keepYLable) {
    plot(minProp, minTime, ylim=c(minProp, maxProp), xlim=c(minTime, maxTime), type = "n", main = title,
       xlab = if(plotXLable) "Time after simulation's start" else NA,
       ylab = yLable,
       xaxt='n',
       cex.main=0.9)
  } else {
    plot(minProp, minTime, ylim=c(minProp, maxProp), xlim=c(minTime, maxTime), type = "n", main = title,
         xlab = if(plotXLable) "Time after simulation's start" else NA,
         ylab = yLable,
         xaxt='n',
         yaxt = 'n',
         cex.main=0.9)
  }
  
  xDelta <- 3600 * 3
  xpos <- seq(0, maxTime, by=xDelta)
  labels <- sapply(xpos, function(x){toDateString(x)})
  labels[length(labels)] <- "24h"
  axis(side=1, at=xpos, labels=labels)
  
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
  
  if (plotLegend) {
    legend(0, maxY, c("DC1", "DC2"),
         col = colors, lty = ltys, cex=0.7)
  }
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
