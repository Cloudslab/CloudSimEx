args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) >= 1){
  setwd(args[1])
  subDir<-"tmp"
}

source('util.R')
source('parseRubis.R')
source('parseSar.R')
source('parseSimulation.R')

plotWorkload(paste0(subDir, "/Workloads_Compared.pdf"))

plotExp2SimPerfBulk()

outFile <- paste0(subDir, "/EXP2_DC1_DB.test.txt")
compareExp2(sarFile="db_server_local_EXP2", simFile="performance_sessions_DC1_2.csv", vmId=1,
            maxTPSOperations=maxTPS,
            file=outFile, property="percentCPU", type="db")
compareExp2(sarFile="db_server_local_EXP2", simFile="performance_sessions_DC1_2.csv", vmId=1,
            maxTPSOperations=maxTPS,
            file=outFile, property="percentRAM", type="db")
compareExp2(sarFile="db_server_local_EXP2", simFile="performance_sessions_DC1_2.csv", vmId=1,
            maxTPSOperations=maxTPS,
            file=outFile, property="percentIO", type="db")

outFile <- paste0(subDir, "/EXP2_DC2_DB.test.txt")
compareExp2(sarFile="db_server_ec2_EXP2", simFile="performance_sessions_DC1_2.csv", vmId=3,
            maxTPSOperations=maxTPSEC2,
            file=outFile, property="percentCPU", type="db")

#df<-read.csv(file="stat/db_server_ec2_vmstat_EXP2", sep=";")
compareExp2(sarFile="db_server_ec2_EXP2", simFile="performance_sessions_DC1_2.csv", vmId=3,
            maxTPSOperations=maxTPSEC2,
            file=outFile, property="percentRAM", type="db")
compareExp2(sarFile="db_server_ec2_EXP2", simFile="performance_sessions_DC1_2.csv", vmId=3,
            maxTPSOperations=maxTPSEC2,
            file=outFile, property="percentIO", type="db")

outFile <- paste0(subDir, "/EXP2_DC1_AS.test.txt")
compareExp2(sarFile="web_server_local_EXP2", simFile="performance_sessions_DC1_2.csv", vmId=2,
            maxTPSOperations=maxTPS,
            file=outFile, property="percentCPU", type="web")
compareExp2(sarFile="web_server_local_EXP2", simFile="performance_sessions_DC1_2.csv", vmId=2,
            maxTPSOperations=maxTPS,
            file=outFile, property="percentRAM", type="web")

outFile <- paste0(subDir, "/EXP2_DC2_AS.test.txt")
compareExp2(sarFile="web_server_ec2_EXP2", simFile="performance_sessions_DC1_2.csv", vmId=4,
            maxTPSOperations=maxTPSEC2,
            file=outFile, property="percentCPU", type="web")
#df<-read.csv(file="stat/web_server_ec2_vmstat_EXP2", sep=";")
compareExp2(sarFile="web_server_ec2_EXP2", simFile="performance_sessions_DC1_2.csv", vmId=4,
            maxTPSOperations=maxTPSEC2,
            file=outFile, property="percentRAM", type="web")

