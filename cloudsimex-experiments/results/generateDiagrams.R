#Required packages
#install.packages("gdata")

library(gdata)

asciiChar<-function(n){
  rawToChar(as.raw(n), multiple=FALSE)
}

toDateString<-function(sec) {
  #Start the day from 1
  days <- (sec %/% (24 * 3600)) + 1
  hours <-(sec %/% 3600)
  minutes <-sec %/% 60
  rest <-sec %% 60
  
  # Now normalize the values
  hours <- hours %% 24
  minutes <- minutes %% 60
  
  return(sprintf("Day %d; %dh", days, hours))
}

# Save the image ...
saveImageFlag <- FALSE

#A list of the the files we want to parse...
inputFiles <- c(#"BaselineExperiment.log",
                #"ExperimentWithHeavierAS.log",
                #"ExperimentWithHeavierASAndMoreAS.log"
                "ExperimentWithHeavierDB.log",
                "ExperimentWithHeavierDBAndMoreDB.log"
                )
#inputFiles <- list.files(pattern = "\\.log$")
fileNamesToReadableNames = list("BaselineExperiment.log" = "Baseline experiment",
                                "ExperimentWithHeavierAS.log" = "2 app. servers in",
                                "ExperimentWithHeavierASAndMoreAS.log" = "3 app. servers in",
                                "ExperimentWithHeavierDB.log" = "2 app. servers in",
                                "ExperimentWithHeavierDBAndMoreDB.log" = "3 app. servers in"
                                )

lblCode = 97 # 97 is the code for 'a'

numDiagrams <- 2*length(inputFiles)
layout(matrix(1:numDiagrams, numDiagrams/2, 2, byrow = TRUE)) # Grid 2x2 layout

xLable <- "Time after start"
yLable <- "Session Delay in seconds"

#The delta between the X axes
xDelta <- 3600 * 5

if (saveImageFlag) {
  pdf(file='RPlot.pdf')
  svg()
}

for(file in inputFiles) {
  print(file)
  initStat <- read.table(file, header=T, sep=";", stringsAsFactors = FALSE)
  stat <- subset(initStat, trim(Complete) == 'true')
  
  # Get the data centers by the DB vm ids, since they are unique within a datacenter
  statDc1 <- subset(stat, DbVmId == 1)
  statDc2 <- subset(stat, DbVmId == 2)
  
  print(paste("DC1 ", mean(statDc1$Delay)))
  print(paste("DC2", mean(statDc2$Delay)))
  
  maxY <- max(stat$Delay)
  maxX <- max(stat$StartTime)
  #The positions to draw the x axes
  xpos <- seq(0, maxX, by=xDelta)
  titleFactor <- 1
  
  title1 = paste("(", asciiChar(lblCode), ") ", 
                 fileNamesToReadableNames[[file]], " data centre 1", sep = "")
  lblCode <- lblCode + 1
  title2 = paste("(", asciiChar(lblCode), ") ", 
                 fileNamesToReadableNames[[file]], " data centre 2", sep = "")
  lblCode <- lblCode + 1
  
  plot(statDc1$Delay ~ statDc1$StartTime, type = "l", main = title1,
       xlab = xLable, ylab = yLable, ylim=c(0, maxY), xaxt='n',cex.main=titleFactor)
  axis(side=1, at=xpos, labels=toDateString(xpos))
  
  plot(statDc2$Delay ~ statDc2$StartTime, type = "l", main = title2,
       xlab = xLable, ylab = yLable, ylim=c(0, maxY), xaxt='n',cex.main=titleFactor) 
  axis(side=1, at=xpos, labels=toDateString(xpos))
}

# Will close the stream to the image files..
if(saveImageFlag) {
  dev.off()
}
