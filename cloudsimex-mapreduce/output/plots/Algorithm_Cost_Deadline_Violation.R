require(ggplot2);
require(reshape);
experimentName <- "Algorithm_Cost_Deadline";
csvData <- read.csv(paste(c(experimentName,".csv"),collapse = ''),header=F);
rownames(csvData) <- csvData [,1];
csvData[,1] <- NULL;
csvData <- csvData[-1,];
csvData <- csvData[,colSums(is.na(csvData))<nrow(csvData)];
violation <- c(
  (length(csvData[1,][csvData[1,] == -1])/ncol(csvData))*100,
  (length(csvData[2,][csvData[2,] == -1])/ncol(csvData))*100,
  (length(csvData[3,][csvData[3,] == -1])/ncol(csvData))*100,
  (length(csvData[4,][csvData[4,] == -1])/ncol(csvData))*100,
  (length(csvData[5,][csvData[5,] == -1])/ncol(csvData))*100,
  (length(csvData[6,][csvData[6,] == -1])/ncol(csvData))*100);
pdf(paste(c(experimentName,"_Violation.pdf"),collapse = ''),width=10);
barplot(violation,ylab="SLA Violation Percentage", names.arg=rownames(csvData));
dev.off();