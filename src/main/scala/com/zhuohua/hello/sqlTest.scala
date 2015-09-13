package com.zhuohua.hello

/**
 * Created by pc on 2015/8/10.
 */

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class User(id: Int, name: String)

class sqlTest {

}

object sqlTest {
  def main(args: Array[String]) {
    import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val loggger = LogManager.getRootLogger()
    loggger.setLevel(Level.WARN)
    
    val sc = new SparkContext(new SparkConf().setAppName("we").setMaster("local[4]"))
    val rawUsersRDD = sc.textFile("src/main/resources/users-sm.csv")
    val ss = Array("2", "2")
    // files.exists()
    val usersRDD = rawUsersRDD.map { user =>
      val tokens = user.split(",")
      User(tokens(0).toInt, tokens(1).toString)
    }
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val userdata = usersRDD.toDF()
    userdata.registerTempTable("usertable")
    val usersql = sqlContext.sql("select *  from usertable")
    val data = usersql.take(4)
    usersql.take(4)
    //    customerDF.select(customerDF("id").as("Customer ID"),
    //  customerDF("discount").as("Total Discount")).show()
    val userid = usersql.select(usersql("id"))
    userid.take(4)

    val ratingsCsvRDD = sc.textFile("src/main/resources/ratings.csv.gz")
    val ratingsJsonRDD = ratingsCsvRDD.map(rating => {
      val tokens = rating.split(",")
      s"""{"fromUserId":${tokens(0)},"toUserId":${tokens(1)},"rating":${tokens(2)}}"""
    })
    ratingsJsonRDD.take(1)
    val ratingsJsonSchemaRDD = sqlContext.jsonRDD(ratingsJsonRDD)
    val test = sqlContext.read
    val datareader2 = test.json(ratingsJsonRDD)
    //val datareader3

    ratingsJsonSchemaRDD.registerTempTable("ratingsJsonTable")
    sqlContext.sql("DESCRIBE ratingsJsonTable")
    //val test2=sqlContext.w
    //Show the top 10 most-active users
    val mostActiveUsersSchemaRDD = sqlContext.sql("SELECT fromUserId, count(*) as ct from ratingsJsonTable group by fromUserId order by ct desc limit 10")
    mostActiveUsersSchemaRDD.collect()
    //old api
    mostActiveUsersSchemaRDD.saveAsParquetFile("src/main/output/testsql/")
    //new api
    mostActiveUsersSchemaRDD.write.parquet("src/main/output/testsql2/")
  }
}
