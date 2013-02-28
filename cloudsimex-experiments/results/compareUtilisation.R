source('parseSar.R')

dbBaseline <- "db_server_0"
dbServersFiles <- list("db_server_0"
                       , "db_server_1"
                       , "db_server_5"
                       , "db_server_10"
                       , "db_server_100"
                       , "db_server_200"
                       , "db_server_300"
                       , "db_server_500"
                       #, "db_server_1000"
                       )
webBaseline <- "web_server_0"
webServersFiles <- list("web_server_0"
                        , "web_server_1"
                        , "web_server_5"
                        , "web_server_10"
                        , "web_server_100"
                        , "web_server_200"
                        , "web_server_300"
                        , "web_server_500"
                        #, "web_server_1000"
                        )

maxTPS <- 1000


prepareSessionData <- function(type, size = 10, step = 5, ram = 512, cpu = 1000, io = 1000) {
  perfFile <- ""
  baseLineFile <- ""
  if(type == "web") {
    perfFile <- paste0("web_server_", size)
    baseLineFile <- paste0("web_server_", 0)
  } else {
    perfFile <- paste0("db_server_", size)
    baseLineFile <- paste0("db_server_", 0)
  }
  sessionsFile <- paste0("sessions_", size, ".txt")
  
  baseLineFrame <- parseSar(baseLineFile)
  perfFrame <- prepareSarFrame(parseSar(perfFile), baseLineFrame)
  sessionsFrame <- read.csv(sessionsFile)
  
  timeSpan <- max(sessionsFrame$endTime) - min(sessionsFrame$startTime)
  perfFrame <- perfFrame[perfFrame$Time < timeSpan,]
  
  # Get only the columns we need
  columns <-if(type == "web") {
    c("Time", "%CPUUtil", "%SessionMem")
  } else {
    c("Time", "%CPUUtil", "%SessionMem", "%tps")  
  }
  perfFrame <- perfFrame[,(names(perfFrame) %in% columns)]
  
  #Convert in form suitable for CloudSim
  perfFrame[,"%CPUUtil"] <- (perfFrame[,"%CPUUtil"] * cpu * step) / (100 * size)
  perfFrame[,"%CPUUtil"] <- sapply(perfFrame[,"%CPUUtil"], function(x){if(x == 0) 1 else x})
  
  perfFrame[,"%SessionMem"] <- (perfFrame[,"%SessionMem"] * ram) / (100 * size)
  if(type == "db") {
    perfFrame[,"%tps"] <- (perfFrame[,"%tps"] * io * step) / (100 * size)
  }
  
  #Get only the data for the seconds we need
  perfFrame <- perfFrame[seq(1, nrow(perfFrame), step),]
  perfFrame <- perfFrame[complete.cases(perfFrame),]
  
  newNamesVector <- c("Time"="Time", "%CPUUtil"="CLOUDLET_MIPS", "%SessionMem"="CLOUDLET_RAM", "%tps"="CLOUDLET_IO")
  colWidth <- 15
  colJustification <- "right"
  names(perfFrame) <- sapply(names(perfFrame), function(n) {format(newNamesVector[n], justify = colJustification, width = colWidth)})
  
  for (name in names(perfFrame)) {
    perfFrame[, name] <- sapply(perfFrame[, name], function(s) {format(s, justify = colJustification, width = colWidth)} )
  }
  
  write.csv(perfFrame,
              file=paste0(type, "_cloudlets.txt"),
              row.names = FALSE,
              quote=FALSE) 
}


# Typical proerties - %memused, "%CPUUtil", UsedMem, tps
plotComparison <- function(type, property="%CPUUtil", useColors = T) {
  pattern <- paste0(type, "_server_\\d+$")
  files <- list.files()[grep(pattern, list.files()) ]
  baseLineFile <- paste0(type, "_server_0")
  
  colors= if(!useColors) {
    sapply(1:length(files), function(x){"black"})
  } else {
    rainbow(length(files))
  }
  
  baseLineFrame <- parseSar(baseLineFile)
  
  frames <- lapply(files, function(f){prepareSarFrame(parseSar(f), baseLineFrame)})
  minTime <- min(sapply(frames, function(fr) {min(fr$Time)} ) )
  maxTime <- max(sapply(frames, function(fr) {max(fr$Time)} ) )

  minProp <- 0
  maxProp <- 100
  if (!grepl("^%", property)) {
    minProp <- min(sapply(frames, function(fr) {min(as.numeric(fr[, property]))} ) )
    maxProp <- max(sapply(frames, function(fr) {max(as.numeric(fr[, property]))} ) )
    print(c(minProp, maxProp))
  }
  
  plot(minProp, minTime, ylim=c(minProp, maxProp), xlim=c(minTime, maxTime), type = "n", main = property,
       xlab = "Time in seconds",
       ylab = property)

  linetype <- c(1:length(files))
  plotchar <- seq(18, 18 + length(files), 1)
  
  i <- 1
  for(frame in frames) {
    print(paste("Summary for :", property, "in", files[i]))
    print(summary(as.numeric(frame[,property])))
    
    lines(frame[,property]~frame$Time,  type="l", col=colors[i], lty = linetype[i], pch = plotchar[i])
    i<- i + 1
  }
  
  legend(0, maxProp, files, col = colors, lty = linetype, pch = plotchar, cex=0.8)
}

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
  
  df[,"ActiveMem"] = abs((as.numeric(df$kbactive) - as.numeric(baseLineFrame$kbactive)))
  df[,"%ActiveMem"] = 100 * df[,"ActiveMem"] / df[,"KBMemory"]
  
  df[,"%tps"] = 100 * as.numeric(df[,"tps"]) / maxTPS
  
  df
}


