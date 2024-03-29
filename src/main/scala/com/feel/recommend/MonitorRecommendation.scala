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
         .filter(_(0).toInt >= REAL_USER_ID_BOUND)
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
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => {
      val user = x.head
      val attribution = x.tail.toSet
      (user, attribution)
    })

    val recommendationLikedStatics = recommendation.join(likedAttributionRDD)
    .map(x => {
      val user = x._1
      val allRecommendationNumber = x._2._1.size + 0.0
      val likedNumber = (x._2._2 & x._2._1).size + 0.0 // likedSet & allRecommendationSet
      (user, likedNumber + "\t" + allRecommendationNumber + "\t" + likedNumber / allRecommendationNumber)
    })

    recommendationLikedStatics.saveAsTextFile(args(2))
    recommendationLikedStatics.map(x => (x._2.split("\t").head, 1))
    .reduceByKey((a, b) => a + b)
    .saveAsTextFile(args(3)) // recommendationLikedStatics

    likedAttributionRDD.map(x => (x._2.size, 1)).reduceByKey((a, b) => a + b).saveAsTextFile(args(4))

    val userGender = sc.textFile(args(5))
    .map(_.split("\t"))
    .filter(_.length == 2)
    .filter(_(0).toInt >= REAL_USER_ID_BOUND)
    .map(x => (x(0), x(1)))

    val genderRecommendationLikedStatics = userGender.join(recommendationLikedStatics)
    .map(x => {
      val gender = x._2._1
      val number = x._2._2.split("\t").head
      (gender + "," + number, 1)
    })
    .reduceByKey((a, b) => a + b)

    genderRecommendationLikedStatics.saveAsTextFile(args(6))
  }
}