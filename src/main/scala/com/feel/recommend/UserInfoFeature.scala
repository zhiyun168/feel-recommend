package com.feel.recommend

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.HashMap

/**
 * Created by canoe on 7/24/15.
 */

object UserInfoFeature {

  private val REAL_USER_ID_BOUND = 1075

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val userInfoRDD = sc.textFile(args(0))

    val userFeatureRDD = userInfoRDD
      .map(_.replaceAll("[()]", "").split(","))
      .map(x => (x(0), x(1)))
      .groupByKey()
      .map(x => {
      val userAttribution = new HashMap[String, String]()
      val user = x._1
      x._2.foreach(attribution => {
        val tmp = attribution.split(":")
        userAttribution(tmp(0)) = tmp(1)
      })
      (user, userAttribution)
    })

    val userGender = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1)))

    val userFollowingAverageFeature = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1))) // leader, follower
      .join(userGender) // leader, (follower, leaderGender)
      .join(userFeatureRDD) // leader, ((follower, leaderGender), leaderFeature)
      .map(x => (x._2._1, (x._1, x._2._2)))
      .groupByKey()
      .map(f = x => {
      val user = x._1._1 + "\t" + x._1._2
      val featureMapList = x._2.map(_._2)

      def featureAverage(feature: String, featureMapList: Iterable[HashMap[String, String]]) = {
        if (featureMapList.size != 0)
          featureMapList.foldLeft(0D)((acc, hash) => acc + hash(feature).toDouble) / featureMapList.size
        else
          0D
      }

      val ageAverage = featureAverage("age", featureMapList)
      val followingRatioAverage = featureAverage("followingRatio", featureMapList)

      val mostTag = featureMapList.foldLeft(new HashMap[String, Int]())((count, hash) => {
        hash.get("mostTag").toString.split("|").foreach(tag => {
          if (count.get(tag).isEmpty) {
            count(tag) = 1
          } else {
            count(tag) += 1
          }
        })
        count
      }).toArray.sortWith(_._2 > _._2).map(_._1).take(3)
      (user, (ageAverage, followingRatioAverage, mostTag))
    })

    userFollowingAverageFeature.saveAsTextFile(args(3))
  }
}
