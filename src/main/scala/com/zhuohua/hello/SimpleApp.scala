package com.zhuohua.hello

/* SimpleApp.scala */

import org.apache.spark.{SparkConf, SparkContext}

//log4j Level Order: ALL < TRACE < DEBUG < INFO < WARN < ERROR < FATAL < OFF
//https://logging.apache.org/log4j/2.0/manual/architecture.html
//以下有多种方法设置spark程序log的级别
//待解决： 在阅读spark源码中如何修改log4j？log4j_4()有问题

object SimpleApp {
 def main(args: Array[String]) {
   log4j_1()
   //log4j_2()
   //log4j_3()
   //log4j_4()
 }

  /**
   Output: no additional logs: 
  2015-08-10 20:27:44,830 [main] WARN  org.apache.hadoop.util.NativeCodeLoader - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
  2015-08-10 20:27:44,832 [main] WARN  org.apache.hadoop.io.compress.snappy.LoadSnappy - Snappy native library not loaded
  2015-08-10 20:27:45,162 [main] WARN  root - LOG2: Lines with a: 15, Lines with b: 8
   */
  def log4j_1() = {
    import org.apache.log4j.{Level, LogManager,PropertyConfigurator}
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val logger = LogManager.getRootLogger()
    logger.setLevel(Level.WARN)

    val logFile = "src/main/resources/test.log"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    
    val numCs = logData.filter(line => line.contains("b")).count()
    println(numCs)
    logger.warn("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    logger.info("LOG1: Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    logger.warn("LOG2: Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }


  /**
  Output: no additional logs
     http://syshell.net/2015/02/03/spark-configure-and-use-log4j/

  2015-08-10 20:19:13,194 [main] WARN  org.apache.hadoop.util.NativeCodeLoader - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
  2015-08-10 20:19:13,199 [main] WARN  org.apache.hadoop.io.compress.snappy.LoadSnappy - Snappy native library not loaded
  2015-08-10 20:19:13,517 [main] WARN  root - Lines with a: 15, Lines with b: 8
    
    */
  def log4j_2() = {
    import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val loggger = LogManager.getRootLogger()
    loggger.setLevel(Level.WARN)

    val logFile = "src/main/resources/test.log"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    loggger.warn("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }



  /**
  Output: use spark's default log4j template file

  2015-08-10 20:36:43,443 [sparkDriver-akka.actor.default-dispatcher-4] INFO  akka.event.slf4j.Slf4jLogger - Slf4jLogger started
  2015-08-10 20:36:43,510 [sparkDriver-akka.actor.default-dispatcher-3] INFO  Remoting - Starting remoting
  2015-08-10 20:36:43,718 [sparkDriver-akka.actor.default-dispatcher-5] INFO  Remoting - Remoting started; listening on addresses :[akka.tcp://sparkDriver@Zhuohua-PC:59112]
  2015-08-10 20:36:43,721 [sparkDriver-akka.actor.default-dispatcher-5] INFO  Remoting - Remoting now listens on addresses: [akka.tcp://sparkDriver@Zhuohua-PC:59112]
  2015-08-10 20:36:43,976 [main] INFO  org.eclipse.jetty.server.Server - jetty-8.1.14.v20131031
  2015-08-10 20:36:43,997 [main] INFO  org.eclipse.jetty.server.AbstractConnector - Started SocketConnector@0.0.0.0:59116
  2015-08-10 20:36:44,368 [main] INFO  org.eclipse.jetty.server.Server - jetty-8.1.14.v20131031
  2015-08-10 20:36:44,380 [main] INFO  org.eclipse.jetty.server.AbstractConnector - Started SelectChannelConnector@0.0.0.0:4040
  2015-08-10 20:36:45,081 [main] WARN  org.apache.hadoop.util.NativeCodeLoader - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
  2015-08-10 20:36:45,082 [main] WARN  org.apache.hadoop.io.compress.snappy.LoadSnappy - Snappy native library not loaded
  2015-08-10 20:36:45,092 [main] INFO  org.apache.hadoop.mapred.FileInputFormat - Total input paths to process : 1
  2015-08-10 20:36:45,364 [main] WARN  org.apache.spark - Lines with a: 15, Lines with b: 8
    */
  def log4j_3() = {
    import org.apache.log4j.{Level, Logger}

    val logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.WARN)

    val logFile = "src/main/resources/test.log"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    logger.warn("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

  /**
  Output: no additional logs
     http://www.cnblogs.com/hseagle/p/4423398.html

  2015-08-10 20:19:13,194 [main] WARN  org.apache.hadoop.util.NativeCodeLoader - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
  2015-08-10 20:19:13,199 [main] WARN  org.apache.hadoop.io.compress.snappy.LoadSnappy - Snappy native library not loaded
  2015-08-10 20:19:13,517 [main] WARN  root - Lines with a: 15, Lines with b: 8

    */
  def log4j_4() = {
    import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
    PropertyConfigurator.configure("src/main/resources/log4j-trace.properties")
    val loggger = LogManager.getRootLogger()
    loggger.setLevel(Level.ERROR)

    val logFile = "src/main/resources/test.log"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    loggger.error("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

}
