source('parseSar.R')

dbServersFiles <- list(db_server_10,
                       db_server_100,
                       db_server_100)

webServersFiles <- list(web_server_10,
                        web_server_100,
                        web_server_100)

plotCPU <- function(){
  
  for(file in webServersFiles) {
    df <- parseSar(file)
  
  }
  
  
}





