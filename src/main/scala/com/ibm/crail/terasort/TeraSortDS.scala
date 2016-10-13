package com.ibm.crail.terasort

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by atr on 13.10.16.
  */
object TeraSortDS {

  case class KVRecord(key: Array[Byte], value: Array[Byte])

  def main (args: Array[String]) {
    doSortWithOpts(args,None)
  }

  def doSortWithOpts (args: Array[String], scin: Option[SparkContext]): Unit = {

    val options = new ParseTeraOptions
    options.parse(args)

    println(" ##############################")
    println(" This is the test code that take parameters.")
    options.show_help()
    options.showOptions()
    println(" ##############################")

    val inputFile = options.getInputDir
    val outputFile = options.getOutputDir

    //spark session is about SQL
    val sparkSession:SparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    val sc: SparkContext  = sparkSession.sparkContext
    /* now we set the params */
    options.setSparkOptions(sc.getConf)

    /* now we first read in stuff */

    try {
      if(options.isPartitionSet) {
        val splitsize = options.getParitionSize
        sc.hadoopConfiguration.set(FileInputFormat.SPLIT_MINSIZE, splitsize.toString)
        sc.hadoopConfiguration.set(FileInputFormat.SPLIT_MAXSIZE, splitsize.toString)
      }
      /* see if we want to sync the output file */
      sc.hadoopConfiguration.setBoolean(TeraOutputFormat.FINAL_SYNC_ATTRIBUTE,
        options.getSyncOutput)

      val beg = java.lang.System.currentTimeMillis()
      val rawInputData = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile)
      val setting = options.showOptions()
      println(setting)
      val recordDF = rawInputData.toDF()
      recordDF.printSchema()

      //casting it explicitly does not work becuse () vs individual stuff (_1, _2)
      val recordDS = sparkSession.createDataset(rawInputData)
      recordDS.printSchema()
      val sortOutputDS = recordDS.sort("_1")
      val sortOutputRDD = sortOutputDS.rdd
      //sortOutputDS.show()
      val items = sortOutputRDD.count()
      println(" Total stuff contains : " + items + " rows")
      val end = java.lang.System.currentTimeMillis()
      println(setting)
      println("-------------------------------------------")
      println("Execution time: %.3fsec".format((end-beg)/1000.0) + " partition size was: " + sortOutputRDD.partitions.length)
      println("-------------------------------------------")

      // we can even write a parquet format 
      sortOutputRDD.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile)

    } finally {
      sparkSession.stop()
    }
  }

}
