source('parseSar.R')

dbBaseline <- "db_server_0"
dbServersFiles <- list("db_server_0",
                       "db_server_1",
                       "db_server_5",
                       "db_server_10",
                       "db_server_100",
                       "db_server_200"
                       #, "db_server_1000"
                       )
webBaseline <- "web_server_0"
webServersFiles <- list("web_server_0",
                        "web_server_1",
                        "web_server_5",
                        "web_server_10",
                        "web_server_100",
                        "web_server_200"
                        #, "web_server_1000"
                        )

maxTPS <- 1000

#colors = rainbow(length(files))
#colors <- c("black", "red", "green", "blue")

# Typical proerties - %memused, "%CPUUtil", UsedMem, tps
plotComparison <- function(property="%CPUUtil", baseLineFile, files, useColors = T) {
  colors= if(!useColors) {
    sapply(1:length(files), function(x){"black"})
  } else {
    rainbow(length(files))
  }
  
  baseLineFrame <- parseSar(baseLineFile)
  
  frames <- lapply(files, function(f){prepareFrame(parseSar(f), baseLineFrame)})
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

prepareFrame <- function(df, baseLineFrame) {
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


