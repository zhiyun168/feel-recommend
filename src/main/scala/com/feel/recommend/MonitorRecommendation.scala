package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by canoe on 7/20/15.
 */
object MonitorRecommendation {

  private val REAL_USER_ID_BOUND = 1075

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    def getAttributionRDD(arg: String) = {
       sc.textFile(arg)
         .map(_.split("\t"))
         .filter(_.length == 2)
         .filter(_(0).toInt <= REAL_USER_ID_BOUND)
         .map(x => (x(0), x(1))) // user, attribute
         .reduceByKey((a, b) => a + "\t" + b)
         .map(x => {
         val user = x._1
         val attribution = x._2.split("\t").toSet
         (user, attribution)
       })
    }

    val likedAttributionRDD = getAttributionRDD(args(0))

    val recommendation = sc.textFile(args(1))
      .map(_.replaceAll("[()a-zA-Z \\t]", "").split(","))
      .filter(_.length >= 2)
      .filter(_(0).toInt <= REAL_USER_ID_BOUND)
      .map(x => {
      val user = x.head
      val attribution = x.tail.toSet
      (user, attribution)
    })

    val recommendationLikedRatio = recommendation.join(likedAttributionRDD)
    .map(x => {
      val user = x._1
      val allRecommendationNumber = x._2._1.size + 0.0
      val likedNumber = (x._2._2 & x._2._1).size + 0.0 // likedSet & allRecommendationSet
      (user, likedNumber + "\t" + allRecommendationNumber + "\t" + likedNumber / allRecommendationNumber)
    })

    recommendationLikedRatio.saveAsTextFile(args(2))
  }
}