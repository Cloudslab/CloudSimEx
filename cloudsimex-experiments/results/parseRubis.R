source('parseSar.R')
source('util.R')


# Prints the delays of all sessions form RUBIS
printDelays <- function(baseSize) {
  files <- getFiles(sessionPattern)
  
  baseLineFile <- paste0(subDir, "/sessions_", baseSize, ".txt")
  
  baseLineFrame <- read.csv(baseLineFile)
  
  for(file in files) {
    if(file != baseLineFile) {
      frame <- read.csv(file)
      frame$delay <- frame$duration - mean(baseLineFrame$duration)
      print(paste0(file, ":"))
      print(summary(frame$delay))
    }
  }
}

# Creates the files, defining the session behaviour
prepareSessionData <- function(size = 100, step = 5, ram = 512, cpu = 10000, io = 10000, stepFunc=meanOfMeanAndMed) {
  print("Generating AS server data")
  prepareSessionDataForType(type = "web", size=size, step=step, ram=ram, cpu=cpu, io=io, stepFunc=stepFunc)
  print("")
  print("Generating DB server data")
  prepareSessionDataForType(type = "db", size=size, step=step, ram=ram, cpu=cpu, io=io, stepFunc=stepFunc)
}

# Creates the file, defining the session behaviour for the db or web server, as
# specified by the type parameter, which should be either "db" or "web"
prepareSessionDataForType <- function(type, size = 50, step = 5, ram = 512, cpu = 10000, io = 10000, stepFunc=meanOfMeanAndMed, maxY=40) {
  # Get the names of the files to use for baselining and performance characteristics
  perfFile <- ""
  baseLineFile <- ""
  if(type == "web") {
    perfFile <- paste0(subDir, "/web_server_", size)
    baseLineFile <- paste0(subDir, "/web_server_", baseLineSize)
  } else {
    perfFile <- paste0(subDir, "/db_server_", size)
    baseLineFile <- paste0(subDir, "/db_server_", baseLineSize)
  }
  
  # Parse the files...
  baseLineFrame <- parseSar(baseLineFile)
  perfFrame <- prepareSarFrame(parseSar(perfFile), baseLineFrame)
  
  timeSpan <- getSessionLastTimeByWorkload(size)
  
  # Remove the perormance characteristics outside the timespan
  perfFrame <- perfFrame[perfFrame$Time < timeSpan,]
  
  # Get only the columns we need - remove the others
  columns <-if(type == "web") {
    c("Time", "%CPUUtil", "%SessionMem", "%ActiveMem")
  } else {
    c("Time", "%CPUUtil", "%SessionMem", "%ActiveMem", "%tps")  
  }
  perfFrame <- perfFrame[,(names(perfFrame) %in% columns)]
  perfFrame[,"%CPUUtil"] <- (perfFrame[,"%CPUUtil"] * cpu * step) / (100 * (size - baseLineSize))
  perfFrame[,"%CPUUtil"] <- sapply(perfFrame[,"%CPUUtil"], function(x){if(x < 1) 1 else x })
  perfFrame[,"%SessionMem"] <- (perfFrame[,"%SessionMem"] * ram) / (100 * (size - baseLineSize))
  perfFrame[,"%ActiveMem"] <- (perfFrame[,"%ActiveMem"] * ram) / (100 * (size - baseLineSize))
  
  perfFrame[,"%CPUUtil"] <- averageSteps(perfFrame[,"%CPUUtil"], step, stepFunc = stepFunc)
  perfFrame[,"%CPUUtil"] <- averageSteps(perfFrame[,"%CPUUtil"], step, stepFunc = stepFunc)
  perfFrame[,"%SessionMem"] <- averageSteps(perfFrame[,"%SessionMem"], step, stepFunc = stepFunc)
  perfFrame[,"%ActiveMem"] <- averageSteps(perfFrame[,"%ActiveMem"], step, stepFunc = stepFunc) 
  
  
  if(type == "db") {
    perfFrame[,"%tps"] <- (perfFrame[,"%tps"] * io * step) / (100 * size)
    perfFrame[,"%tps"] <- averageSteps(perfFrame[,"%tps"], step, stepFunc = stepFunc)
    perfFrame[,"%tps"] <- sapply(perfFrame[,"%tps"], function(x){if(x < 1) round(x) else x})
  }
  
  #Get only the data for the seconds(steps) we need
  perfFrame <- perfFrame[seq(1, nrow(perfFrame), step),]
  perfFrame <- perfFrame[complete.cases(perfFrame),]
  
  #Print the summary of the stats
  for(name in columns) {
    print(paste("Summary for", name)) 
    print(summary(perfFrame[, name]))
  }
  
  #Prepare the format for the result file
  newNamesVector <-
    c("Time"="Time", "%CPUUtil"="CLOUDLET_MIPS", "%ActiveMem"="CLOUDLET_RAM", "%SessionMem"="USED_RAM", "%tps"="CLOUDLET_IO")
  colWidth <- 15
  colJustification <- "right"
  names(perfFrame) <- sapply(names(perfFrame), function(n) {format(newNamesVector[n], justify = colJustification, width = colWidth)})
  
  for (name in names(perfFrame)) {
    perfFrame[, name] <- sapply(perfFrame[, name], function(s) {format(s, justify = colJustification, width = colWidth)} )
  }
  
  write.csv(perfFrame,
              file=paste0(subDir, "/", type, "_cloudlets.txt"),
              row.names = FALSE,
              quote=FALSE) 
}

# Plots the utilisation of different properties
# Typical proerties - %memused, "%CPUUtil", UsedMem, tps
plotComparison <- function(type, property="%CPUUtil", useColors = T, plotLegend = T, forWorkload = "All", maxY = NA, file = NA) {
  propertiesToNames <- c("%CPUUtil" = "%CPU utilisation", "%ActiveMem" = "%RAM utilisation", "%tps" = "%Disk utilisation")
  
  benchPattern <- paste0(type, "_server_\\d+$")
  if(forWorkload[1] == "All") {
    forWorkload <- sapply (getFiles(sessionPattern), getNumbersInNames )
  } 
  
  benchFiles <-sapply(forWorkload, function(w) {paste0(subDir,"/", type,"_server_",w)})
  sessionFiles <- sapply(forWorkload, function(w) {paste0(subDir, "/sessions_", w, ".txt")})
  
  baseLineFile <- paste0(subDir, "/", type, "_server_", baseLineSize)
  
  colors= if(!useColors) {
    sapply(1:length(benchFiles), function(x){"black"})
  } else {
    rainbow(length(benchFiles))
  }
  
  baseLineFrame <- parseSar(baseLineFile)
  
  frames <- lapply(benchFiles, function(f){prepareSarFrame(parseSar(f), baseLineFrame)})
  minTime <- min(sapply(frames, function(fr) {min(fr$Time)} ) )
  maxTime <- max(sapply(sessionFiles, function(f) {getSessionLastTime(f)} ) )
  
  minProp <- 0
  maxProp <- if (is.na(maxY)) 100 else maxY
  if (!grepl("^%", property)) {
    minProp <- min(sapply(frames, function(fr) {min(as.numeric(fr[, property]))} ) )
    maxProp <- if (is.na(maxY)) max(sapply(frames, function(fr) {max(as.numeric(fr[, property]))})) else maxY
    print(c(minProp, maxProp))
  }
  
  openGraphsDevice(file)
  fullScreen(hasTitle=T)
  
  title <- paste(propertiesToNames[property], "for", paste(sort(forWorkload), collapse = ', '), "sessions")
  plot(minProp, minTime, ylim=c(minProp, maxProp), xlim=c(minTime, maxTime), type = "n", main = title,
       xlab = "Time in seconds",
       ylab = propertiesToNames[property])

  linetype <- c(1:length(benchFiles))
  plotchar <- seq(18, 18 + length(benchFiles), 1)
  
  i <- 1
  for(frame in frames) {
    print(paste("Summary for :", property, "in", benchFiles[i]))
    print(summary(as.numeric(frame[,property])))
    
    lines(frame[,property]~frame$Time,  type="l", col=colors[i], lty = linetype[i], pch = plotchar[i])
    i<- i + 1
  }
  
  if (plotLegend) {
    legend(0, maxProp, sapply(benchFiles, 
           function(f) {paste(getNumbersInNames(f), if(getNumbersInNames(f) == 1) "session" else "sessions" )}),
           col = colors, lty = linetype, cex=0.7)
  }
  
  resetMar()
  closeDevice(file)
}

getSessionLastTimeByWorkload <- function(size) {
  sessionsFile <- paste0(subDir, "/sessions_", size, ".txt")
  getSessionLastTime(sessionsFile)
}

getSessionLastTime <- function(sessionsFile) {
  sessionsFrame <- read.csv(sessionsFile)
  
  # Define the timespan we are interested in
  if(length(sessionsFrame$endTime) > 0) {
    max(sessionsFrame$endTime) - min(sessionsFrame$startTime)
  } else {
    0
  }
}
