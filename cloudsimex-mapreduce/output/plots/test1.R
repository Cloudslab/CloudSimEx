require(ggplot2);
require(reshape);
experimentName <- "test1";
csvData <- read.csv(paste(c(experimentName,".csv"),collapse = ''),header=F);
rownames(csvData) <- csvData [,1];
csvData [,1] <- NULL;
csvData <- t(csvData );
csvData <- as.data.frame(csvData );
csvData[csvData==-1] = NA;
#plot(csvData$Deadline, csvData$LFFCostHybrid, type="l");
csvData <- melt(csvData ,  id = 'Deadline', variable_name = 'Algorithms');
#ggplot(csvData, aes(Deadline,value)) + geom_line(aes(colour = Algorithms));
ggplot(csvData, aes(Deadline,value)) + geom_line() + facet_grid(Algorithms ~ .)