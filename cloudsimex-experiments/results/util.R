# Common utility functions and constants used throughout teh statistical analysis

subDir <- "stat"
maxTPS <- 1000
baseLineSize <- 1

sessionPattern <- "sessions_\\d+.txt$"
simSessionPattern <- "simulation_sessions_\\d+.csv$"

initialMar<-par("mar")

# Chrecks if the string represents a valid numerical
check.num <- function(N){
  length(grep("^\\d+(\\.\\d+)?$", as.character(N))) != 0
}

# Chrecks if the string represents a valid date in the form DD:DD:DD
check.date <- function(d){
  length(grep("^\\d{2}:\\d{2}:\\d{2}$", as.character(d))) != 0
}

averageSteps <- function(lst, step, stepFunc = median) {
  idx <- 1: length(lst)
  sapply(idx, lambdaAverageSteps, lst, step, stepFunc=stepFunc)
  #lst
}

lambdaAverageSteps <- function(i, lst, step, stepFunc = median) {
  #zero-based index
  indx <- i-1
  from <- (indx %/% step) * step + 1
  end <- min((indx %/% step + 1) * step, length(lst))
  
  stepFunc(lst[from: end])
}

getFiles <- function(pattern) {
  files <- list.files(subDir)[grep(pattern, list.files(subDir))]
  files <- sapply(files, function(f){paste0(subDir, "/", f)})
  
  numbersInNames <- getNumbersInNames(files) 
  files <- files[order(numbersInNames)]
  
  as.vector(files)
}

getNumbersInNames <- function(names) {
  sapply(names, function(n) {as.numeric(gsub("[^0-9]", "", n))})
}


# Returns the mean of the mean and the median
meanOfMeanAndMed <- function(lst) {
  med = median(lst)
  mn = mean(lst)
  mean(c(med, mn))
}

# Returns the mean after removing the smallest and biggest element.
# If the size of the list is less than 4 - then only the mean is returned.
stepFuncDefault <- function(lst) {
  result <- if(length(lst) >= 4){
    maxEl <- max(lst)
    minEl <- min(lst)
    
    middle <- lst[c(-(which(lst == maxEl)[1]), -(which(lst == minEl)[1]))]
    mean(middle)
  } else {
    mean(lst) 
  }
  
  result
}

resetMar <- function() {
  par(mar = initialMar)
}

fullScreen <- function(hasTitle = F) {
  top <- if (hasTitle) { initialMar[3] } else { 0 }
  newMar <- c(initialMar[1] - 1, initialMar[2], top, initialMar[4] - 2)
  #print(newMar)
  par(mar=newMar)
}

#Prepares to write the file. If file is NA does nothing 
openGraphsDevice <- function(file) {
  ext = if(is.na(file)) {
    ""
  } else {
    getExtension(file)
  }
    
  if(ext == "pdf") {
    pdf(file)
  } else if(ext == "wmf") {
    win.metafile(file)
  } else if(ext == "png") {
    png(file)
  } else if(ext == "jpeg") {
    jpeg(file)
  } else if(ext == "bmp") {
    bmp(file)
  } else if(ext == "ps") {
    postscript(file)
  } else if(ext == "svg") {
    svg(file)
  }
}

#Closes the graphical device/stream associated with this file.
# If the file is NA - nothing happens
closeDevice <- function(file) {
  if(!is.na(file)) {
    dev.off()
  }
}

# Resets all parameters to their defaults
resetPar <- function() {
  dev.new()
  op <- par(no.readonly = TRUE)
  dev.off()
  
  initialMar<-par("mar")
  
  op
}

getExtension <- function(file) {
  parts <- strsplit(file, "\\.")[[1]]
  parts[length(parts)]
}
