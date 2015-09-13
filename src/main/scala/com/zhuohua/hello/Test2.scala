package com.zhuohua.hello

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Zhuohua on 12/09/2015.
 */

private case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

object Test2 {
  def main(args: Array[String]): Unit ={
    
    hello
    //transformTest1
    log4j_1
  }
  
  def hello():Unit = {
    println("Hello, worldw.")
  }

  def stddevFunc(c: Column): Column =
  // val dtaa= avg(c * c)
    sqrt(avg(c * c) - (avg(c) * avg(c)))
  
  def transformTest1() : Unit = {

    import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val logger = LogManager.getRootLogger()
    logger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("DataFrame-Transform").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val myFunc = udf { (x: Double) => x + 1}

    val customerDF = sc.parallelize(custs, 4).toDF()
    val colNames = customerDF.columns
    val cols = colNames.map(cName => customerDF.col(cName))
    val theColumn = customerDF("discount")

    val mappedCols = cols.map(c => if (c.toString() == theColumn.toString()) myFunc(c).as("transformed") else c)

    customerDF.groupBy("state").agg($"state", stddevFunc($"discount")).show()
    val datas = customerDF.groupBy("state").count()

    logger.warn("End of transform")
  }


  def log4j_1() = {
/*    import org.apache.log4j.{Level, LogManager}
    val logger = LogManager.getRootLogger()
    logger.setLevel(Level.WARN)

    val logFile = "src/main/resources/test.log"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)*/

    import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val logger = LogManager.getRootLogger()
    logger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("DataFrame-Transform").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logFile = "src/main/resources/test.log"
    val logData = sc.textFile(logFile, 2).cache()
    
    val numOfLine = logData.count()
    logger.warn("Number of lines: %s".format(numOfLine))
    //val numAs = logData.filter(line => line.contains("a")).count()
    //val numBs = logData.filter(line => line.contains("b")).count()
    //logger.warn("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    //logger.info("LOG1: Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    //logger.warn("LOG2: Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  } 
  
}
