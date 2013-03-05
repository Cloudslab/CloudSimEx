source('parseSar.R')

maxTPS <- 1000
maxIOPS <- 1000
ram <- 512

subDir <- "stat"

# Prints the delays of all sessions form RUBIS
printDelays <- function(baseSize) {
  pattern <- "sessions_\\d+.txt$"
  files <- getFiles(pattern)
  
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
prepareSessionData <- function(size = 50, step = 5, ram = 512, cpu = 1000, io = 1000, stepFunc=mean) {
  print("Generating AS server data")
  prepareSessionDataForType(type = "web", size=size, step=step, ram=ram, cpu=cpu, io=io, stepFunc=stepFunc)
  print("")
  print("Generating DB server data")
  prepareSessionDataForType(type = "db", size=size, step=step, ram=ram, cpu=cpu, io=io, stepFunc=stepFunc)
}

# Creates the file, defining the session behaviour for the db or web server, as
# specified by the type parameter, which should be either "db" or "web"
prepareSessionDataForType <- function(type, size = 50, step = 5, ram = 512, cpu = 1000, io = 1000, stepFunc=mean) {
  # Get the names of the files to use for baselining and performance characteristics
  perfFile <- ""
  baseLineFile <- ""
  if(type == "web") {
    perfFile <- paste0(subDir, "/web_server_", size)
    baseLineFile <- paste0(subDir, "/web_server_", 0)
  } else {
    perfFile <- paste0(subDir, "/db_server_", size)
    baseLineFile <- paste0(subDir, "/db_server_", 0)
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
  perfFrame[,"%CPUUtil"] <- (perfFrame[,"%CPUUtil"] * cpu * step) / (100 * size)
  perfFrame[,"%CPUUtil"] <- sapply(perfFrame[,"%CPUUtil"], function(x){if(x < 1) 1 else x})
  perfFrame[,"%SessionMem"] <- (perfFrame[,"%SessionMem"] * ram) / (100 * size)
  perfFrame[,"%ActiveMem"] <- (perfFrame[,"%ActiveMem"] * ram) / (100 * size)
  
  perfFrame[,"%CPUUtil"] <- averageSteps(perfFrame[,"%CPUUtil"], step, stepFunc = stepFunc)
  perfFrame[,"%CPUUtil"] <- averageSteps(perfFrame[,"%CPUUtil"], step, stepFunc = stepFunc)
  perfFrame[,"%SessionMem"] <- averageSteps(perfFrame[,"%SessionMem"], step, stepFunc = stepFunc)
  perfFrame[,"%ActiveMem"] <- averageSteps(perfFrame[,"%ActiveMem"], step, stepFunc = stepFunc) 
  
  
  if(type == "db") {
    perfFrame[,"%tps"] <- (perfFrame[,"%tps"] * io * step) / (100 * size)
    perfFrame[,"%tps"] <- averageSteps(perfFrame[,"%tps"], step, stepFunc = stepFunc)
    perfFrame[,"%tps"] <- sapply(perfFrame[,"%tps"], function(x){if(x < 1) 1 else x})
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
              file=paste0("stat/", type, "_cloudlets.txt"),
              row.names = FALSE,
              quote=FALSE) 
}

# Plots the utilisation of different properties
# Typical proerties - %memused, "%CPUUtil", UsedMem, tps
plotComparison <- function(type, property="%CPUUtil", useColors = T, plotLegend = T, forWorkload = "All", maxY = NA) {
  benchPattern <- paste0(type, "_server_\\d+$")
  sessionPattern <- paste0("sessions_\\d+.txt$")
  if(forWorkload[1] == "All") {
    benchFiles <- getFiles(benchPattern)
    sessionFiles <- getFiles(sessionPattern)
  } else {
    benchFiles <-sapply(forWorkload, function(w) {paste0("stat/", type,"_server_",w)})
    sessionFiles <- sapply(forWorkload, function(w) {paste0("stat/", "sessions_", w, ".txt")})
  }
  baseLineFile <- paste0(subDir, "/", type, "_server_0")
  
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
  
  plot(minProp, minTime, ylim=c(minProp, maxProp), xlim=c(minTime, maxTime), type = "n", main = property,
       xlab = "Time in seconds",
       ylab = property)

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
    legend(0, maxProp, benchFiles, col = colors, lty = linetype, cex=0.7)
  }
}

# Based on the provided SAR frames - one from a baseline (sessions with 0 or so users)
# and the other workload session (e.g. with 100 users) precomputes some utilisation 
# properties/columns like: %CPUUtil, %UsedMem, %SessionMem, %ActiveMem and  %tps
prepareSarFrame <- function(df, baseLineFrame) {
  # Make the time start from 0
  df$Time = df$Time - min(df$Time)
  df[,"%CPUUtil"] = 100 - as.numeric(df[,"%idle"])
  
  df[,"KBMemory"] = as.numeric(df$kbmemused) + as.numeric(df$kbmemfree);
  df[,"UsedMem"] = as.numeric(df$kbmemused) - as.numeric(df$kbcached) - as.numeric(df$kbbuffers)
  df[,"%UsedMem"] = 100 * df[,"UsedMem"] / df[,"KBMemory"]
  
  baseLineFrame[,"UsedMem"] = as.numeric(baseLineFrame$kbmemused) - as.numeric(baseLineFrame$kbcached) - as.numeric(baseLineFrame$kbbuffers)
  df[,"SessionMem"] = abs((as.numeric(df$UsedMem) - as.numeric(baseLineFrame$UsedMem)))
  df[,"%SessionMem"] = 100 * df[,"SessionMem"] / (df[,"KBMemory"] - as.numeric(baseLineFrame$UsedMem))
  
  df[,"ActiveMem"] = abs(as.numeric(df$kbactive) - as.numeric(baseLineFrame$kbactive))
  df[,"%ActiveMem"] = 100 * df[,"ActiveMem"] / df[,"KBMemory"]
  
  df[,"%tps"] = 100 * as.numeric(df[,"tps"]) / maxTPS
  
  df
}

lambdaAverageSteps <- function(i, lst, step, stepFunc = median) {
  #zero-based index
  indx <- i-1
  from <- (indx %/% step) * step + 1
  end <- min((indx %/% step + 1) * step, length(lst))
  
  stepFunc(lst[from: end])
}

averageSteps <- function(lst, step, stepFunc = median) {
  idx <- 1: length(lst)
  sapply(idx, lambdaAverageSteps, lst, step, stepFunc=stepFunc)
  #lst
}

getFiles <- function(pattern) {
  files <-sort(list.files(subDir)[grep(pattern, list.files(subDir))])
  files <- sapply(files, function(f){paste0(subDir, "/", f)})
  
  numbersInNames <- sapply(files, function(f) {as.numeric(gsub("[^0-9]", "", f))}) 
  files <- files[order(numbersInNames)]
  
  files
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

meanOfMeanAndMed <- function(lst) {
  med = median(lst)
  mn = mean(lst)
  mean(c(med, mn))
}
