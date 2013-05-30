require(ggplot2);
require(reshape);
experimentName <- "Algorithm_ExecutionTime_Budget";
csvData <- read.csv(paste(c(experimentName,".csv"),collapse = ''),header=F);
rownames(csvData) <- csvData [,1];
csvData [,1] <- NULL;
csvData <- t(csvData );
csvData <- as.data.frame(csvData );
csvData[csvData==-1] = NA;
#plot(csvData$Budget, csvData$LFFCostHybrid, type="l");
csvData <- melt(csvData ,  id = 'Budget', variable_name = 'Algorithms');
ggplot(csvData, aes(Budget,value)) + geom_line() + facet_grid(Algorithms ~ .)