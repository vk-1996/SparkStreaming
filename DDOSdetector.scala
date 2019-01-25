
package com.vk.codes

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import org.apache.log4j.Level
import java.util.regex.Pattern
import java.util.regex.Matcher


object DDOSdetector {
  def Logging() = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)   
  }
   
  def apacheLogPattern():Pattern = {
    val ddd = "\\d{1,3}"                      
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"  
    val client = "(\\S+)"                     
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"              
    val request = "\"(.*?)\""                 
    val status = "(\\d{3})"
    val bytes = "(\\S+)"                     
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)    
  }
 
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "DDOSdetector", Seconds(1))
    
    Logging()
    
    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via ncat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    
    // Extract the IP address field from each log line
    val request_ip = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(1)})
    
    // Reduce by IP addresses over a 5-minute window sliding every second
    val Counts = request_ip.map(x => (x.toString(), 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(120), Seconds(1))
    
    // Sort and print the results
    val sortedResults = Counts.transform(rdd => rdd.sortBy(x => x._2, false))
    //sortedResults.print()
    // filter ip address that tried to access server more that 200 times in last 2 minutes 
    val ddos_ips =  sortedResults.transform(rdd=> rdd.filter(x=> x._2 > 200))
    ddos_ips.print()
    //sortedResults.saveAsTextFiles("file:///C:/Users/vanketesh.kumar/Desktop/ipaddresses/DDOS_attack")
    
    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

