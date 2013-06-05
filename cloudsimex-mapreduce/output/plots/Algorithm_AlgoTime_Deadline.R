require(ggplot2);
require(reshape);
experimentName <- "Algorithm_AlgoTime_Deadline";
csvData <- read.csv(paste(c(experimentName,".csv"),collapse = ''),header=F);
rownames(csvData) <- csvData [,1];
csvData [,1] <- NULL;
csvData <- t(csvData );
csvData <- as.data.frame(csvData );
csvData[csvData==-1] = NA;
#plot(csvData$Deadline, csvData$LFFCostHybrid, type="l");
csvData <- melt(csvData ,  id = 'Deadline', variable_name = 'Algorithms');
csvData <- rename(csvData, c("value"="Algorithm_Running_Time"));
pdf(paste(c(experimentName,".pdf"),collapse = ''));
ggplot(csvData, aes(Deadline,Algorithm_Running_Time)) + geom_line() + facet_grid(Algorithms ~ .);
dev.off()