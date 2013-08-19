source('util.R')

# Parses the output of SAR (System Activity Monitor)
# Returns a data frame for analysis with all the data.
parseSar <- function(fileName) {
  con <- file(fileName) 
  open(con);

  frameLine <- 1
  header <- list()
  values <- list()
  
  result <- NULL 
  isPrevLineHeader <- FALSE
  lastHeaderTime <- NULL
  
  # Read and skip the first line with the system info
  line <- readLines(con, n = 1, warn = FALSE)
  print(paste0("System Info: " , line))
  
  #Parse lines one by one
  while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    # Skip empty lines
    if(line[1] == "") {
      next
    }
    
    elements <- parseLine(line)
    isHeader <- isHeaderLine(elements)
    lineTime <- as.POSIXlt(strptime(elements[1], "%H:%M:%S"))
        
    # If it does not start with time - skip it, because it is an average.
    # If it is a second value line after a header - skip it as well
    if (is.na(lineTime) || (!isHeader && !isPrevLineHeader)) {
      next
    }
    
    if (is.null(lastHeaderTime) && isHeader) {
      lastHeaderTime <- lineTime
    }
    
    # Upon a change in the time of the headers, or whe headers start
    # repeating - put things in the data frame
    if (isHeader && (lineTime != lastHeaderTime || elements[2] %in% header) ) {
      # If frame does not exists - create it
      if(is.null(result)) {
        result <- data.frame(t(rep(NA, length(header) )))  
        header[1] <- "Time"
        names(result) <- header
      }
      
      result[frameLine, ] <- values
      
      # Print a status message, so that user knows we are not blocked
      if(frameLine %% 200 == 0) {
        print(paste0(fileName, ": ", frameLine, " rows have been generated ..."))
      }
      
      lastHeaderTime <- lineTime
      frameLine <- frameLine + 1
      
      header <- list()
      values <- list()
    }
    
    # If we are appending to existing values - then remove the fist element
    if ((isHeader && length(header) != 0) || (!isHeader && length(values) != 0)) {
      elements <- elements[-1]
    }
    
    if (isHeader) {
      header <- append(header, elements)
    } else {
      values <- append(values, elements)
    } 
    isPrevLineHeader <- isHeader
  } 
  
  close(con)
  #print(result$Time)
  result$OrigTime <- result$Time
  result$Time <- sapply(result$Time,
                        function(e){as.POSIXct(strptime(e, "%H:%M:%S"))} )
  #
  
  dayTime <- 24 * 60 * 60
  
  for(i in 1:nrow(result)) {
    while(i > 1 && result$Time[i-1] > result$Time[i]) {
      result[i, "Time"] = result[i, "Time"] + dayTime 
    }
  }
  
  result$ReadableTime = sapply(result$Time, function(t) {toDateString(t, mask=c(T, T, T, F))} ) 
  
  return (result)
}

isHeaderLine <- function(elementsList){
  for(el in elementsList) {
    if(check.num(el)) {
      return (FALSE)
      break
    }
  }
  TRUE
}

parseLine <- function(line, firstTime=NA) {
  elements <- unlist(strsplit(line, split="\\s+"))
  if (elements[2] %in% c("AM", "PM")) {
    h <- as.numeric(substr(elements[1], 1, 2))
    if( (elements[2] == "PM" && h < 12) || (elements[2] == "AM" && h == 12) ) {
       h <- (h + 12) %% 24
       h <- if (h == 0) "00" else h
       elements[1] <- gsub("^\\d{2}", h, elements[1])
    }
    elements <- elements[-2]
  }
  elements
}


# Based on the provided SAR frames - one from a baseline (sessions with 0 or so users)
# and the other workload session (e.g. with 100 users) precomputes some utilisation 
# properties/columns like: %CPUUtil, %UsedMem, %SessionMem, %ActiveMem and  %tps
prepareSarFrame <- function(df, baseLineFrame, maxTPSOps=maxTPS) {
  # Make the time start from 0
  df$Time = df$Time - min(df$Time)
  
  baseLineFrame[, "%realIdle"] = as.numeric(baseLineFrame[,"%idle"])  + as.numeric(baseLineFrame[,"%iowait"])
  baseLineFrame[,"%CPUUtil"] = 100 - baseLineFrame[,"%realIdle"] 
  
  df[,"%realIdle"] = as.numeric(df[,"%idle"]) + as.numeric(df[,"%iowait"])
  df[,"%CPUUtil"] = 100 - df[,"%realIdle"] - mean(baseLineFrame[,"%CPUUtil"])
  df[,"%CPUUtil"] = sapply(df[,"%CPUUtil"], function(x) {if (x < 0) 0 else x})
  
  df[,"KBMemory"] = as.numeric(df$kbmemused) + as.numeric(df$kbmemfree);
  df[,"UsedMem"] = as.numeric(df$kbmemused) - as.numeric(df$kbcached) - as.numeric(df$kbbuffers)
  df[,"%UsedMem"] = 100 * df[,"UsedMem"] / df[,"KBMemory"]
  
  baseLineFrame[,"UsedMem"] = as.numeric(baseLineFrame$kbmemused) - as.numeric(baseLineFrame$kbcached) - as.numeric(baseLineFrame$kbbuffers)
  df[,"SessionMem"] = abs((as.numeric(df$UsedMem) - as.numeric(baseLineFrame$UsedMem)))
  df[,"%SessionMem"] = 100 * df[,"SessionMem"] / (df[,"KBMemory"] - as.numeric(baseLineFrame$UsedMem))
  
  df[,"ActiveMem"] = abs(as.numeric(df$kbactive) - as.numeric(baseLineFrame$kbactive))
  df[,"%ActiveMem"] = 100 * df[,"ActiveMem"] / df[,"KBMemory"]
  
  df[,"%tps"] = 100 * as.numeric(df[,"tps"]) / maxTPSOps
  
  df
}

prepareSarFrame0 <- function(df, maxTPSOps=maxTPS, type, activeMem=NA) {
  baseLineFile <- paste0(subDir, "/", type, "_server_", baseLineSize)
  baseLineFrame <- parseSar(baseLineFile)
  baseMem <- mean(as.numeric(baseLineFrame$kbactive))
  print(paste("Base Memory is ", baseMem))
  
  # Make the time start from 0
  df$Time = df$Time - min(df$Time)
  
  df[, "%realIdle"] = as.numeric(df[,"%idle"])  + as.numeric(df[,"%iowait"]) + as.numeric(df[,"%steal"])
  df[, "%owned"] = 100 - as.numeric(df[,"%steal"])
  df[,"%CPUUtil"] = 100 - df[,"%realIdle"] 
  df[,"%CPUUtil"] = 100 * df[,"%CPUUtil"] / df[, "%owned"]
  
  df[,"%CPUUtil"] = sapply(df[,"%CPUUtil"], function(x) {if (x < 0) 0 else x})
  
  df[,"KBMemory"] = as.numeric(df$kbmemused) + as.numeric(df$kbmemfree);
  df[,"UsedMem"] = as.numeric(df$kbmemused) - as.numeric(df$kbcached) - as.numeric(df$kbbuffers)
  df[,"%UsedMem"] = 100 * df[,"UsedMem"] / df[,"KBMemory"]
  
  df[,"SessionMem"] = abs(as.numeric(df$UsedMem))
  df[,"%SessionMem"] = 100 * df[,"SessionMem"] / (df[,"KBMemory"])
  
  df[,"ActiveMem"] = abs(as.numeric(df$kbactive) - baseMem) 
  df[,"ActiveMem"] = sapply(df[,"ActiveMem"], function(x) {if(x < 0) 0 else x } )
  df[,"ActiveMem"] = if (is.na(activeMem)) df[,"ActiveMem"] else abs(activeMem[1:nrow(df)] - baseMem)
  df[,"%ActiveMem"] = 100 * df[,"ActiveMem"] / df[,"KBMemory"]
  
  df[,"%tps"] = 100 * as.numeric(df[,"tps"]) / maxTPSOps
  
  df
}
