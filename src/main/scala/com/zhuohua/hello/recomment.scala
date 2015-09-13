package com.zhuohua.hello


import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

/**
 * Created by pc on 2015/8/14.
 */
class recomment {
}
object recomment {
  def main(args: Array[String]) {
    import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
    PropertyConfigurator.configure("src/main/resources/log4j.properties")
    val loggger = LogManager.getRootLogger()
    loggger.setLevel(Level.WARN)
    
    val sc = new SparkContext(new SparkConf().setAppName("we").setMaster("local[4]"))
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    //ad_recommend_video_watch_items_500
    val ad_recommend_imei_video_watch_features = hiveContext.sql("select imei ,feature from algo.ad_recommend_imei_video_watch_features_500 where stat_date=20150814")
    val ad_recommend_video_watch_items_500 = hiveContext.sql("select code ,name,column_index from algo.ad_recommend_video_watch_items_500 where stat_date=20150814")
    //视频名称和column_index
    val ad_recommend_video_watch_items_500map = ad_recommend_video_watch_items_500.map(row => (row.getLong(2), row.getString(1))).collectAsMap()
    val feature = ad_recommend_imei_video_watch_features.map(row => (row.getString(0), row.getString(1))).zipWithIndex()
    val imewithId = feature.map {
      case (x, y) =>
        (x._1, y)
    }.collectAsMap()
    val feature2 = feature.flatMap {
      case (featurs, numbersId) =>
        val data = featurs._2.split(" ")
        val data2 = data.map {
          line =>
            val data3 = line.split(":")
            (numbersId.toInt, data3(0).toInt, data3(1).toDouble)
        }
        data2
    }

    val countuserlook=feature2.map(line=>(line._1,line._3)).reduceByKey(_+_).filter(_._2==1.0).count()//2014368
    //3308007   //5322375
    val countuserlook2=feature2.map(line=>(line._1,line._3)).filter(_._2==1.0).distinct().count()//74697663
//视频看的数量排序
    val productcount=feature2.map(_._2).countByValue().toList.sortBy(_._2).reverse
    val productcount2=feature2.map(_._2).countByValue().toList.sortBy(_._2).filter(_._2<1910)
println(productcount2.mkString("\n"))
    val productcountname=productcount.map(x=>(ad_recommend_video_watch_items_500map(x._1),x._2))

        println(  productcount.take(100).mkString("\n"))

    val productwithone=productcount.filter(_._2==1).size
    feature2.take(100).foreach(println)
    val ratingdata = feature2.map {
      case (user, iterm, ratas) =>
        Rating(user, iterm, ratas)
    }
    val numRating = ratingdata.count()//74697663
    val numUsers = ratingdata.map(_.user).distinct().count()//5322375
    val numMovies = ratingdata.map(_.product).distinct().count()//39104
    println(s"$numRating \t  $numUsers \t $numMovies")
    feature2.count()//74697663

    val splits = ratingdata.randomSplit(Array(0.8, 0.2))
    val traing = splits(0)
    val testdata = splits(1)
    val testdatanum = testdata.count()//14940972
    //(1544972,32.0), (3545608,67.0), (4018960,47.0)
    testdata.filter(_.user==1544972).map(r=>(ad_recommend_video_watch_items_500map(r.product))).collect()
    ratingdata.filter(_.user==1544972).map(r=>(ad_recommend_video_watch_items_500map(r.product))).collect()

    val countuserlook3=feature2.map(line=>(line._1,line._3)).reduceByKey(_+_).filter(_._2>20.0)
    val countuserlookname=feature2.map(line=>(line._1,line._3)).reduceByKey(_+_).filter(_._2>20.0)

    countuserlook3.take(10)

    val model = new ALS().setRank(100).setIterations(10).setLambda(0.02).setImplicitPrefs(true).setUserBlocks(20).setProductBlocks(100).run(traing)
    val result=computeRmse(model,testdata)
//对某个用户进行推荐

    val userfactor=model.userFeatures
    val itemId=110
    val itemFactor=model.userFeatures.lookup(itemId).head
    val itemVector=new DoubleMatrix(itemFactor)
    val sim=model.userFeatures.map{case (id,factor)=>
     val factorVector=new DoubleMatrix(factor)
        val sims=cosineSimilarity(factorVector,itemVector)
      (id,sims)
    }

    val sortedSim=sim.top(10)(Ordering.by[(Int, Double), Double]{
      case(id, similarity) => similarity
    })
    sortedSim.take(10).mkString("\n")
    val sortedSimname=sortedSim.map{case(id,sim)=>
      (ad_recommend_video_watch_items_500map(id),sim)
    }
    println(sortedSimname.take(10).mkString("\n"))

    model.userFeatures.count() //4889700
    val productfactor=model.productFeatures.count() //39102
   // val predictrating=model.predict(159,12)
    val userId =1544972 //159 // 4444    /136  /55   //888   //369  // 4446  // 7879  //4444  //136
    val K = 10
    val topicrecomment=model.recommendProducts(userId,K)
    println(topicrecomment.mkString("\n"))
    val moviesForUser=ratingdata.keyBy(_.user).lookup(userId)
    val moviesForUserlook=ratingdata.keyBy(_.user).lookup(userId).map(_.product).toList
    val userlookMoviename=moviesForUser.sortBy(-_.rating).take(10).map(rating => (ad_recommend_video_watch_items_500map(rating.product), rating.rating)).toList
    userlookMoviename.foreach(println)
    val recommentuserMoviename =topicrecomment.map(rating => (ad_recommend_video_watch_items_500map(rating.product), rating.rating))
    recommentuserMoviename.foreach(println)

    val recommentuserMovienameunlook =topicrecomment.filter{
      line=>
        if(!moviesForUserlook.contains(line.product)){
          true
        }else{
          false
        }
    }.map{
      rating=>
          (ad_recommend_video_watch_items_500map(rating.product), rating.rating)
    }
    recommentuserMovienameunlook.foreach(println)

    sc.stop()
    //  println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")
  }
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
  def booIexit(userArray:Array[Int], testdata: RDD[Rating], ad_recommend_video_watch_items_500map: Map[Long, String])={
    val data=userArray.flatMap{
      line=>
      val userId= testdata.filter(_.user==line).collect()
      if(userId.size>0){
        val data2=userId.map(r=>(r.product,r.user))
        data2
      }else{
        null
      }
      //data2
    }
  data
  }
  def computeRmse(model: MatrixFactorizationModel, testdata: RDD[Rating]) = {

    def mapPredictedRating(r: Double) = {
      math.max((math.min(r, 1.0)), 0.0)
    }
    val predictions = model.predict(testdata.map(x => (x.user, x.product)))
    val predictionAndRating=predictions.map{
      x=>
        ((x.user,x.product), if(mapPredictedRating(x.rating)>0.5) 1.0 else 0.0)
    }.join(testdata.map(x=>((x.user,x.product),x.rating))).values

    math.sqrt(predictionAndRating.map(x=>(x._1-x._2)*(x._1-x._2)).mean())

   val scores=predictionAndRating.map{
     case(x,y)=>
       if(x==y){
         1
       }else{
         0
       }
   }.sum
    predictionAndRating.take(100)
    val metrics = new BinaryClassificationMetrics(predictionAndRating)
    metrics.areaUnderPR()
    metrics.areaUnderROC()
  }
}
