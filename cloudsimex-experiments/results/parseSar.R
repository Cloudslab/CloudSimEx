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
    
    elements <- unlist(strsplit(line, split="\\s+"))
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
      if(frameLine %% 100 == 0) {
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
  
  result$Time <- sapply(result$Time,
                        function(e){as.POSIXct(strptime(e, "%H:%M:%S"))} )
  
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

check.num <- function(N){
  length(grep("^\\d+(\\.\\d+)?$", as.character(N))) != 0
}

check.date <- function(d){
  length(grep("^\\d{2}:\\d{2}:\\d{2}$", as.character(d))) != 0
}
