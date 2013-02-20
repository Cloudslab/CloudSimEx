source('parseSar.R')

dbServersFiles <- list("db_server_10",
                       "db_server_100",
                       "db_server_1000")

webServersFiles <- list("web_server_10",
                        "web_server_100",
                        "web_server_1000"
                        )

plotCPU <- function(){
  
  plot(1,2, ylim=c(0, 100), xlim=c(1361316536, 1361317019), type = "n",
       main = "title1",
       xlab = "xLable",
       ylab = "yLable")
  colors <- c("red", "green", "blue")
  
  frames <- lapply(webServersFiles, function(f){normaliseTime(parseSar(f))})
  minTime <- min(sapply(frames, function(fr) {min(fr$Time)} ) )
  maxTime <- max(sapply(frames, function(fr) {max(fr$Time)} ) )
  
  plot(0, 0, ylim=c(0, 100), xlim=c(minTime, maxTime), type = "n")
  
  i <- 1
  for(frame in frames) {
    lines(frame[,"%idle"]~frame$Time,  type="l", col=colors[i])
    i<- i + 1
  }
  
}

normaliseTime <- function(df) {
  df$Time = df$Time - min(df$Time)
  df
}



