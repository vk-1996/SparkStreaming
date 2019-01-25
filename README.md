# SparkStreaming
DDOS_detection
Simple Spark streaming application that reads from a file brodcasting msg on port 9999 using ncat
count the ip address over a time frame of 2 minutes
if count of ip address is more than 200 stores it as a text file

enviroment used:
spark version 2.3
scala ide 
ncat.exe
apache access log.txt


scope of this project:
can be integrated with kafka/Flume to read data directly from server
