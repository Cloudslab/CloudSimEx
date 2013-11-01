source('util.R') 
source('parseSimulation.R') 
library(ggplot2)
library(gridExtra)

STATE_SUCCESS = "Served"
STATE_REJECT = "Rejected"
STATE_FAIL = "Failed"

MINUTE = 60.0
HOUR = 60.0 * MINUTE
DAY = 24 * HOUR

init = T

wldf=20
n=1
sla=30
numdb=1
outputDir="multi-cloud-stat"

args <- commandArgs(trailingOnly = TRUE)
print("Arguements:")
print(args)


dataDirName <- function (basedir="multi-cloud-stat", baseline, wldf, sla, n, numdb) {
  alg <- if (baseline) "baseline" else "run"
  #dataDirName <- paste0(basedir, "/", "wldf(", alg, ")-", wldf, "-n-", sla)
  #wldf(%s)-wldf-%d-sla-%d-n-%d-db-%d
  dataDirName <- gettextf("%s/wldf(%s)-wldf-%d-sla-%d-n-%d-db-%d", basedir, alg, wldf, sla, n, numdb)
  return (dataDirName)
}

if(length(args) >= 1){
  #setwd(args[1])
  
  init = T
  wldf = as.numeric(gsub("[^0-9]", "", args[1]))
  n = as.numeric(gsub("[^0-9]", "", args[2]))
  sla = as.numeric(gsub("[^0-9]", "", args[3]))
  numdb = as.numeric(gsub("[^0-9]", "", args[4]))
  outputDir = dataDirName(baseline=F, wldf=wldf, sla=sla, n=n, numdb=numdb)
  baeselinDir = dataDirName(baseline=T, wldf=wldf, sla=sla, n=n, numdb=numdb)
  print(paste("Output dir is:", outputDir))
  #dir.create(file.path(mainDir, subDir), showWarnings = FALSE)
  
  if (!file.exists(outputDir) || !file.exists(baeselinDir)) {
    print("Data is not present... stopping analysis")
    stop(call. = TRUE)
  }
}


hourLabel <- function(time, delta = 4) {
  time = sapply( time, function(x)  {if (x == 24 * HOUR) x - 1 else x})
  from = (time %/% (delta * HOUR)) * delta
  to = from + delta
  return (paste0(from, "h-", to, "h"))
}

defTimeLabel <-function (data, delta = 4) {
  if (nrow(data) > 0) {
    labelList = unique(lapply(seq(0, 23, delta), function(x) {hourLabel(x * HOUR, delta)} ))
    data$TimeLabel <- hourLabel(data$StartTime, delta)
    data$TimeLabel <- factor(data$TimeLabel, levels = as.vector(labelList))
  }
  return (data)
}



readSessionsData <- function(basedir="multi-cloud-stat", baseline=T,
                             wldf=50, n=1, sla=30, numdb=2, location="Euro", provider="EC2") {
  
  meta=if (location == "Euro") "[EU]" else "[US]"
  
  dataDirName <- dataDirName(baseline=baseline, wldf=wldf, n=n, sla=sla, numdb=numdb)
  fileName <- paste0(dataDirName , "/Sessions-Broker", location, provider, ".csv")
  print(paste("Parsing: ", fileName))
  
  data <- read.table(fileName, header = T, stringsAsFactors = F, quote = '"', sep = ";")
  data <- subset(data, StartTime <= DAY) 
  
  data$SumDelay <- data$Delay + data$LatDelay
  
  # Trim strings ...
  for(name in names(data)) {
    if(class(data[, name]) == "character") {
      data[, name] = trim(data[, name]) 
    }
  }
  
  # Set the state of each session...
  data[, "State"] = ifelse(data$Complete == "true", STATE_SUCCESS,
                           ifelse(data$Meta != meta, STATE_REJECT, STATE_FAIL) )
  
  data
}


if(init) {
  
  baselineEuroEC2 = readSessionsData(baseline=T, location="Euro", provider="EC2", wldf=wldf, n=n,  sla=sla, numdb=numdb)
  baselineEuroGoogle = readSessionsData(baseline=T, location="Euro", provider="Google", wldf=wldf, n=n,  sla=sla, numdb=numdb)
  baselineUSEC2 = readSessionsData(baseline=T, location="US", provider="EC2", wldf=wldf, n=n,  sla=sla, numdb=numdb)
  baselineUSGoogle = readSessionsData(baseline=T, location="US", provider="Google", wldf=wldf, n=n,  sla=sla, numdb=numdb)
  
  runEuroEC2 = readSessionsData(baseline=F, location="Euro", provider="EC2", wldf=wldf, n=n,  sla=sla, numdb=numdb)
  runEuroGoogle = readSessionsData(baseline=F, location="Euro", provider="Google", wldf=wldf, n=n,  sla=sla, numdb=numdb)
  runUSEC2 = readSessionsData(baseline=F, location="US", provider="EC2", wldf=wldf, n=n,  sla=sla, numdb=numdb)
  runUSGoogle = readSessionsData(baseline=F, location="US", provider="Google", wldf=wldf, n=n,  sla=sla, numdb=numdb)
}


adjustPlot <- function (plot, plotX=F, plotY=F, title=NULL, yLim=NA, plotLegend=T, yLab=NULL) {
  plot <- plot +
    theme_bw() + 
    theme(axis.text.x = if(plotX) element_text(angle = 35, hjust = 1, vjust = 1, size = 10) else element_blank(),
          axis.text.y = if(plotY) element_text(angle = 90, hjust = 0.5, size = 10) else element_blank(),
          axis.title.x = if(plotX) element_text(size = 15) else element_blank(),
          axis.title.y = if(plotY) element_text(size = 15) else element_blank(),
          legend.key.size = unit(0.3, "cm"),
          plot.title = element_text(size = 10),
          strip.text.x = element_text(size = 9.5, angle = 0)) + 
    xlab(NULL) +
    ylab(if (plotY) yLab else NULL) +
    ggtitle(title)
  
  #if(yLim != NA) plot = plot + ylim(0,50000)
  if(!is.na(yLim)) {
    plot = plot + scale_y_continuous(limits=c(0,yLim))
  }
  if(!plotY) {
    plot = plot + theme(axis.ticks.y = element_blank())
  }
  if(!plotX) {
    plot = plot + theme(axis.ticks.x = element_blank())
  }
  if(!plotLegend) {
    plot = plot + guides(fill=FALSE)
  }
  return (plot)
}

plotLatencies <- function (file=NA) {
  
  baseline <- rbind(baselineEuroEC2, baselineEuroGoogle, baselineUSEC2, baselineUSGoogle)
  run <- rbind(runEuroEC2, runEuroGoogle, runUSEC2, runUSGoogle)
  
  baseline$AlgLabel <- "Baseline"
  run$AlgLabel <- "Current Work"
  data <- rbind(baseline, run)
  
  # The bars of the whiskers
  whiskers <- with(boxplot(Latency ~ AlgLabel, data = data),
                   data.frame(AlgLabel = names,
                              lower = stats[1, ],
                              upper = stats[5, ]))
  openSizedGraphsDevice(file, width=20, height=7)

  plot <- ggplot(data) + 
    geom_boxplot(aes(x = AlgLabel, y = Latency), 
                 width=0.5, position = position_dodge(width=0.2), 
                 outlier.shape=NA, outlier.size=2) + 
    geom_segment(data = whiskers, aes(x = as.numeric(AlgLabel) - 0.1,
                                      y = lower,
                                      xend = as.numeric(AlgLabel) + 0.1,
                                      yend = lower)) +
    geom_segment(data = whiskers, aes(x = as.numeric(AlgLabel) - 0.1,
                                      y = upper,
                                      xend = as.numeric(AlgLabel) + 0.1,
                                      yend = upper)) 
    
  
  # Define the y-biundaries of the boxplots
  ylim1 = boxplot.stats(baseline$Latency)$stats[c(1, 5)]
  ylim2 = boxplot.stats(run$Latency)$stats[c(1, 5)]
  ylim = c(min(ylim1[1], ylim2[1]) * 0.8, max(ylim1[2], ylim2[2]) * 1.1)
  
  p <- adjustPlot(plot=plot, 
                  plotX=T, plotY=T, title=NULL, yLim=NA, plotLegend=F, yLab="Latency") + 
    theme(axis.text.x = element_text(angle = 0, hjust = 0.5, vjust=1, size = 15),
          axis.text.y = element_text(angle = 0, hjust = 0.5, size = 15),
          axis.title.x = element_text(size = 20),
          axis.title.y = element_text(size = 20)) +  
    #coord_cartesian(ylim = ylim) +
    stat_summary(aes(x = AlgLabel, y = Latency), fun.y = "mean", geom = "point", shape= 23, size= 2, fill="black") +     
    coord_flip(ylim = ylim)
  
  grid.arrange(p)
  closeDevice(file)
}

plotSessionsSumary <- function(baselineDf, runDf, delta = 4, title = "Title...",
                               plotLegend=T, plotY=T, plotX=T, yLim = NA, debug = F) {
  df<-data.frame(cat= NA, state= NA, time = NA, value = NA)
  labelList <- list()
  i <- 1
  for(t in seq(delta, 24, delta)){
    from<-(t-delta) * HOUR
    to <- t * HOUR
    
    label <- paste0(toDateString(from),"-", if (t == 24) "24h" else toDateString(to))
    labelList <- append(labelList, label)
    for (s in c(STATE_SUCCESS, STATE_REJECT, STATE_FAIL)) {
      n <- nrow(subset(baselineDf, 
                       (State == s) & (from <= StartTime) & (StartTime < to)))
      if(n >= 0) {
        df[i,] <- c("Baseline", s, label, n) 
        i<-i+1
      }
    }
    for (s in c(STATE_SUCCESS, STATE_REJECT, STATE_FAIL)) {
      n <- nrow(subset(runDf, 
                       subset = State == s & from <= StartTime & StartTime < to))
      if(n >= 0){
        df[i,] <- c("Current Work", s, label, n) 
        i<-i+1
      }
    }
  }
  df$value <- as.numeric(df$value)
  df$time <- factor(df$time, levels = as.vector(labelList))
  
  if(debug) print(df)
  
  plot <- ggplot() +
    geom_bar(data=df,
             aes(y = value, x = cat, fill = state),
             stat="identity", position='stack') +
    facet_grid( ~time)+
    scale_fill_manual(name="Session Outcome: ", values = c("#AA3929", "#F8AF46", "#183324"))
  
  return (adjustPlot(plot=plot, plotX=plotX, plotY=plotY, title=title, yLim=yLim,
                     plotLegend=plotLegend, yLab="# sessions"))
}

#extract legend
#https://github.com/hadley/ggplot2/wiki/Share-a-legend-between-two-ggplot2-graphs
g_legend<-function(a.gplot){
  tmp <- ggplot_gtable(ggplot_build(a.gplot))
  leg <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box")
  legend <- tmp$grobs[[leg]]
  return(legend)
}

plotAllDCsSessionsSummary <- function(delta = 6, file = NA) {
  openSizedGraphsDevice(file, width=20, height=17)
  allFrames <- lst<- list(runEuroEC2, runEuroGoogle, 
                          runUSEC2, runUSGoogle,
                          baselineEuroEC2, baselineEuroGoogle, 
                          baselineUSEC2, baselineUSGoogle)
  barHeigth <- 0
  for(frame in allFrames) {
    for(t in seq(delta, 24, delta)) {
      from <- (t-delta) * HOUR
      to <- t * HOUR
      n <- nrow(subset(frame, (from <= StartTime) & (StartTime < to)))
      if(n > barHeigth) {
        barHeigth <- n
      }
    }
  }
  
  euroEC2 <- plotSessionsSumary(baselineEuroEC2, runEuroEC2, plotLegend=F,
                                title="DC-EU-E", plotY = T, plotX = F, yLim = barHeigth, delta=delta) +
    theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))
  euroGoogle <- plotSessionsSumary(baselineEuroGoogle, runEuroGoogle, plotLegend=F,
                                   title="DC-EU-G", plotY = F, plotX = F, yLim = barHeigth,  delta=delta) +
    theme(plot.margin = unit(c(0, 0, 0, 0.25), "lines"))
  
  usEC2 <- plotSessionsSumary(baselineUSEC2, runUSEC2, plotLegend=F, 
                              title="DC-US-E", plotY = T, plotX = T, yLim = barHeigth, delta=delta) +
    theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))
  usGoogle <- plotSessionsSumary(baselineUSGoogle, runUSGoogle, plotLegend=F,
                                 title="DC-US-G", plotY = F, plotX = T, yLim = barHeigth, delta=delta) +
    theme(plot.margin = unit(c(0, 0, 0, 0.25), "lines"))
  
  widths=c(12, 11)
  grid.arrange(arrangeGrob(euroEC2 + theme(legend.position="none"),
                           euroGoogle + theme(legend.position="none"),
                           ncol=2, widths=widths),
               arrangeGrob(usEC2 + theme(legend.position="none"),
                           usGoogle + theme(legend.position="none"),
                           ncol=2, widths=widths),                 
               g_legend(plotSessionsSumary(baselineEuroEC2, runEuroEC2, plotLegend=T) +  theme(legend.position="bottom")),
               nrow=3, heights=c(16, 20, 2))
  
  closeDevice(file)
}

plotDelaySummary <- function(baselineDf, runDf, delta = 4, title = "Title...",
                             plotY=T, plotX=T, yLim = NA) {
  baselineDf <- defTimeLabel(baselineDf, delta=delta)
  runDf <- defTimeLabel(runDf, delta=delta)
  
  baselineDf$AlgLabel <- "Baseline"
  runDf$AlgLabel <- "Current Work"
  
  merged <- rbind(subset(baselineDf, State == STATE_SUCCESS), 
                  subset(runDf, State == STATE_SUCCESS)) 
  
  plot <- ggplot(merged, aes(x=AlgLabel, y=SumDelay)) + 
    geom_boxplot() + 
    theme_bw() + 
    facet_wrap(~ TimeLabel, nrow=1)
  
  return (adjustPlot(plot=plot, plotX=plotX, plotY=plotY, title=title, yLim=yLim,
                     plotLegend=F, yLab="Delay"))
  + ylab("Delay")
}

plotAllDCsDelaySummary <-function (delta = 6, file = NA, byDC = F) {
  
  #openGraphsDevice(file)
  openSizedGraphsDevice(file, width=20, height=16)
  allFrames <- lst<- list(runEuroEC2, runEuroGoogle, 
                          runUSEC2, runUSGoogle,
                          baselineEuroEC2, baselineEuroGoogle, 
                          baselineUSEC2, baselineUSGoogle)
  maxHeigth <- 0
  for(frame in allFrames) {
    d <- subset(frame, State == STATE_SUCCESS)
    n <- if(nrow(d) > 0) max(d$SumDelay) else 0
    if(n > maxHeigth) {
      maxHeigth <- n
    }
  }
  
  if(byDC == F) {
    euro <- plotDelaySummary(rbind(baselineEuroEC2, baselineEuroGoogle), rbind(runEuroEC2, runEuroGoogle),
                             title="Europe", plotY = T, plotX = F, yLim = maxHeigth, delta=delta) +
      theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))
    
    us <- plotDelaySummary(rbind(baselineUSEC2, baselineUSGoogle), rbind(runUSEC2, runUSGoogle),
                           title="US", plotY = T, plotX = T, yLim = maxHeigth, delta=delta) +
      theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))
    
    grid.arrange(euro, us, nrow=2, heights=c(16, 20))
  } else {
    euroEC2 <- plotDelaySummary(baselineEuroEC2, runEuroEC2,
                                title="Euro EC2", plotY = T, plotX = F, yLim = maxHeigth, delta=delta) +
      theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))
    euroGoogle  <- plotDelaySummary(baselineEuroGoogle, runEuroGoogle,
                                    title="Euro Google", plotY = F, plotX = F, yLim = maxHeigth, delta=delta) +
      theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))
    
    usEC2 <- plotDelaySummary(baselineUSEC2, runUSEC2,
                              title="US EC2", plotY = T, plotX = T, yLim = maxHeigth, delta=delta) +
      theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))
    usGoogle <- plotDelaySummary(baselineUSGoogle, runUSGoogle,
                                 title="US Google", plotY = F, plotX = T, yLim = maxHeigth, delta=delta) +
      theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))
    
    grid.arrange(euroEC2, euroGoogle, usEC2, usGoogle, ncol=2, nrow=2, heights=c(16, 20),  widths=c(11, 9.5))
    
  }
  
  closeDevice(file)
}

summaryDF <- function (names, framesMatrix, from=0, to=24) {
  result <- data.frame(name= NA, overall= NA, success=NA, failed = NA, rejected = NA,
                       successP = NA, failP = NA, rejP = NA, overallP = NA)
  i<-1
  for(frameList in framesMatrix) {
    overall <- 0
    nRow <- 0
    nSuc <- 0
    nRej <- 0
    nFail <- 0 
    for (j in 1:length(frameList)) {
      data <- as.data.frame(frameList[j])
      overall <- overall + nrow(data)
      nRow <- nRow + nrow(subset(data, ((from * HOUR)<= StartTime) & (StartTime <= (to * HOUR))))
      nSuc <- nSuc + nrow(subset(data, 
                                 State == STATE_SUCCESS & (from * HOUR)<= StartTime & StartTime <= (to * HOUR)))
      nRej <- nRej + nrow(subset(data, 
                                 State == STATE_REJECT & (from * HOUR)<= StartTime & StartTime <= (to * HOUR)))
      nFail <- nFail + nrow(subset(data,
                                   State == STATE_FAIL & (from * HOUR)<= StartTime & StartTime <= (to * HOUR))) 
    }
    result[i, ] = c(names[i], nRow, nSuc, nFail, nRej, 
                    paste0(round(100 * nSuc / nRow, 2), "%"),    
                    paste0(round(100 * nFail / nRow, 2), "%"), 
                    paste0(round(100 * nRej / nRow, 2), "%"),
                    paste0(round(100 * nRow / overall, 2), "%"))
    i <- i + 1
  }
  result
}

printSessionsSummary <- function (from=0, to=24) {
  print(paste0("Results for: ", from, "h-", to, "h"))
  print("Each DC")
  names <- c("runEuroEC2", "baselineEuroEC2", "runEuroGoogle", "baselineEuroGoogle",
             "runUSEC2", "baselineUSEC2" , "runUSGoogle", "baselineUSGoogle")
  lst <- list(list(runEuroEC2),
              list(baselineEuroEC2),
              list(runEuroGoogle),
              list(baselineEuroGoogle),
              list(runUSEC2),
              list(baselineUSEC2),
              list(runUSGoogle),
              list(baselineUSGoogle))
  print(summaryDF(names, lst, from=from, to=to))
  
  print("")
  print("Overall by region")
  names <- c("Run-Euro", "Baseline-Euro", "Run-US", "Baseline-US")
  lst <- list(
    list(runEuroEC2, runEuroGoogle), 
    list(baselineEuroEC2, baselineEuroGoogle),
    list(runUSEC2, runUSGoogle),
    list(baselineUSEC2, baselineUSGoogle))
  print(summaryDF(names, lst, from=from, to=to))
  
  print("")
  print("Overall")
  names <- c("Run", "Baseline")
  lst<- list(
    list(runEuroEC2, runEuroGoogle, runUSEC2, runUSGoogle), 
    list(baselineEuroEC2, baselineEuroGoogle, baselineUSEC2, baselineUSGoogle))
  print(summaryDF(names, lst, from=from, to=to))
}


allDCsSessionsSummaryF = paste0(outputDir, "/sessions.pdf")
allDCsDelaySummaryF = paste0(outputDir, "/delaysDCs.pdf")
allRegDelaySummaryF = paste0(outputDir, "/delaysRegions.pdf")
latenciesF = paste0(outputDir, "/latency.pdf")
workloadF = paste0(outputDir, "/workloadFreqs.pdf") 
sessSummaryF = paste0(outputDir, "/workloadFreqs.txt")
delta=4

plotAllDCsSessionsSummary(file=allDCsSessionsSummaryF, delta=delta)
plotAllDCsDelaySummary(file=allDCsDelaySummaryF, byDC = T, delta=delta)
plotAllDCsDelaySummary(file=allRegDelaySummaryF, byDC = F, delta=delta)
plotLatencies(file=latenciesF)
plotWorkload(file=workloadF, wldf=wldf, legendNames=c("EU", "US"), size=c(25, 12)) 
sink(sessSummaryF)
printSessionsSummary()
sink(NULL)

outputArchive <- paste0("multi-cloud-stat/Analysis-wldf-", wldf, "-sla-", sla, "-n-", n, "-numdb-", numdb, ".zip")
zip(outputArchive, files=c(allDCsSessionsSummaryF, allDCsDelaySummaryF, allRegDelaySummaryF,
                           latenciesF, workloadF, sessSummaryF))
