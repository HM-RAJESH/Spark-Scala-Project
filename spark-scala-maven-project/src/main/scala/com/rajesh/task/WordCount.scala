package com.rajesh.task

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCount{
  
  def main(args: Array[String]): Unit = {
    var sparkSession:SparkSession = SparkSession
   .builder()
   .master("local")
   .appName("SparkExample")
   .getOrCreate()

    println("starting word count process ")
    
    //Reading input file and creating rdd with no of partitions 5
    val bookRDD=sparkSession.sparkContext.textFile("inputFiles//wordcount//metamorphosis.txt", 4)
    
    //Regex to clean text
    val pat = """[^\w\s\$]"""
    val cleanBookRDD=bookRDD.map(line=>line.replaceAll(pat, ""))
    
    val wordsRDD=cleanBookRDD.flatMap(line=>line.split(" "))
    
    val wordMapRDD=wordsRDD.map(word=>(word->1))
    
    val wordCountMapRDD=wordMapRDD.reduceByKey(_+_).collect.toMap
    
    wordCountMapRDD.foreach(println)
    
    //wordCountMapRDD.saveAsTextFile("outputfiles//wordCount//wordcount.txt")
    
  }
  
}