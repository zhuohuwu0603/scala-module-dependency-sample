package sparkApp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by pc on 2015/8/20.
 */
class TrasformTest {

}
private case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)
object  TrasformTest {
  def main(args: Array[String]) {




    transformTest1
  }
  def stddevFunc(c: Column): Column =
  // val dtaa= avg(c * c)
  sqrt(avg(c * c) - (avg(c) * avg(c)))

  def transformTest1 {

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
  
//  def 02_basic_spark(): Unit = {
//    wordsList = new Seq('cat', 'elephant', 'rat', 'rat', 'cat')
//
//  }
  
  
}