source('util.R') 
library(ggplot2)
library(gridExtra)

STATE_SUCCESS = "Served"
STATE_REJECT = "Rejected"
STATE_FAIL = "Failed"

MINUTE = 60.0
HOUR = 60.0 * MINUTE
DAY = 24 * HOUR

hourLabel <- function(time, delta = 4) {
  time = sapply( time, function(x)  {if (x == 24 * HOUR) x - 1 else x})
  from = (time %/% (delta * HOUR)) * delta
  to = from + delta
  return (paste0(from, "h-", to, "h"))
}

defTimeLabel <-function (data, delta = 4) {
  labelList = unique(lapply(seq(0, 23, delta), function(x) {hourLabel(x * HOUR, delta)} ))
  data$TimeLabel <- hourLabel(data$StartTime, delta)
  data$TimeLabel <- factor(data$TimeLabel, levels = as.vector(labelList))
  
  return (data)
}

readSessionsData <- function(basedir="multi-cloud-stat", baseline=T,
                             wldf=50, n=30, location="Euro", provider="EC2") {
  meta=if (location == "Euro") "[EU]" else "[US]"
  
  alg <- if (baseline) "baseline" else "run"
  fileName <- paste0(basedir, "/wldf(", alg, ")-", wldf, "-n-", n , "/Sessions-Broker", location, provider, ".csv")
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


init = F

if(init) {
  wldf=200
  n=30
  baselineEuroEC2 = readSessionsData(baseline=T, location="Euro", provider="EC2", wldf=wldf, n=n)
  baselineEuroGoogle = readSessionsData(baseline=T, location="Euro", provider="Google", wldf=wldf, n=n)
  baselineUSEC2 = readSessionsData(baseline=T, location="US", provider="EC2", wldf=wldf, n=n)
  baselineUSGoogle = readSessionsData(baseline=T, location="US", provider="Google", wldf=wldf, n=n)
  
  runEuroEC2 = readSessionsData(baseline=F, location="Euro", provider="EC2", wldf=wldf, n=n)
  runEuroGoogle = readSessionsData(baseline=F, location="Euro", provider="Google", wldf=wldf, n=n)
  runUSEC2 = readSessionsData(baseline=F, location="US", provider="EC2", wldf=wldf, n=n)
  runUSGoogle = readSessionsData(baseline=F, location="US", provider="Google", wldf=wldf, n=n)
}
  
plotSessionsSumary <- function(baselineDf, runDf, delta = 4, title = "Title...",
                               legend=T, plotY=T, plotX=T, yLim = NA, debug = F) {
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
  theme_bw() + 
  facet_grid( ~time) +
  theme(axis.text.x = if(plotX) element_text(angle = 35, hjust = 1, vjust = 1) else element_blank(),
        axis.text.y = if(plotY) element_text() else element_blank(),
        legend.key.size = unit(0.4, "cm"),
        plot.title = element_text(size = 10),
        strip.text.x = element_text(size = 7, angle = 0)) + 
  xlab(NULL) +
  ylab(if (plotY) "# sessions" else NULL) +
  scale_fill_manual(name="Session Outcome: ", values = c("#AA3929", "#F8A31B", "#556670")) +
  ggtitle(title)
  
  #if(yLim != NA) plot = plot + ylim(0,50000)
  if(!is.na(yLim)) plot = plot + scale_y_continuous(limits=c(0,yLim))
  if(!legend) plot = plot + guides(fill=FALSE)
  
  return (plot)
}

#extract legend
#https://github.com/hadley/ggplot2/wiki/Share-a-legend-between-two-ggplot2-graphs
g_legend<-function(a.gplot){
  tmp <- ggplot_gtable(ggplot_build(a.gplot))
  leg <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box")
  legend <- tmp$grobs[[leg]]
  return(legend)
}

plotAllDCsSessionsSummary <- function(delta = 4, file = NA) {
  openGraphsDevice(file)
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
  
  euroEC2 <- plotSessionsSumary(baselineEuroEC2, runEuroEC2, legend=F,
                  title="Euro EC2", plotY = T, plotX = F, yLim = barHeigth, delta) +
    theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))
  euroGoogle <- plotSessionsSumary(baselineEuroGoogle, runEuroGoogle, legend=F,
                  title="Euro Google", plotY = F, plotX = F, yLim = barHeigth,  delta) +
    theme(plot.margin = unit(c(0, 0, 0, 0.25), "lines"))
  
  usEC2 <- plotSessionsSumary(baselineUSEC2, runUSEC2, legend=F, 
                  title="US EC2", plotY = T, plotX = T, yLim = barHeigth, delta) +
    theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))
  usGoogle <- plotSessionsSumary(baselineUSGoogle, runUSGoogle, legend=F,
                  title="US Google", plotY = F, plotX = T, yLim = barHeigth, delta) +
    theme(plot.margin = unit(c(0, 0, 0, 0.25), "lines"))
  
  dummy <- plotSessionsSumary(baselineEuroEC2, runEuroEC2, legend=T) + 
    theme(legend.position="bottom")
  legend <- g_legend(dummy)
  
  widths=c(11, 9.5)
  grid.arrange(arrangeGrob(euroEC2 + theme(legend.position="none"),
                                 euroGoogle + theme(legend.position="none"),
                                 ncol=2, widths=widths),
                     arrangeGrob(usEC2 + theme(legend.position="none"),
                                  usGoogle + theme(legend.position="none"),
                                 ncol=2, widths=widths),                 
                     legend, nrow=3, heights=c(16, 20, 2))
  
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
    facet_wrap(~ TimeLabel, nrow=1) +
    theme(axis.text.x = if(plotX) element_text(angle = 35, hjust = 1, vjust = 1) else element_blank(),
          axis.text.y = if(plotY) element_text() else element_blank(),
          legend.key.size = unit(0.4, "cm"),
          plot.title = element_text(size = 10),
          strip.text.x = element_text(size = 7, angle = 0)) + 
    xlab(NULL) +
    ylab(if (plotY) "Delay" else NULL) +
    scale_fill_manual(name="Session Outcome: ", values = c("#AA3929", "#F8A31B", "#556670")) +
    ggtitle(title)
    
  if(!is.na(yLim)) plot = plot + scale_y_continuous(limits=c(0,yLim))
  
  return (plot)
}

plotAllDCsDelaySummary <-function (delta = 4, file = NA) {
  
  openGraphsDevice(file)
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
  
  euro <- plotDelaySummary(rbind(baselineEuroEC2, baselineEuroGoogle), rbind(runEuroEC2, runEuroGoogle),
                                title="Euro", plotY = T, plotX = F, yLim = maxHeigth, delta) +
    theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))

  us <- plotDelaySummary(rbind(baselineUSEC2, baselineUSGoogle), rbind(runUSEC2, runUSGoogle),
                              title="US EC2", plotY = T, plotX = T, yLim = maxHeigth, delta) +
    theme(plot.margin = unit(c(0, 0, 0, 0), "lines"))
  
  grid.arrange(euro, us, nrow=2, heights=c(16, 20))
  
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

plotAllDCsSessionsSummary(file="multi-cloud-stat/sessions.pdf")
plotAllDCsDelaySummary(file="multi-cloud-stat/delays.pdf")
