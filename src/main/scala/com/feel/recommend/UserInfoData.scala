package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable

/**
 * Created by canoe on 7/23/15.
 */


object UserInfoData {

  private val REAL_USER_ID_BOUND = 1075
  private var PREFIX = "../data/"

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)


    PREFIX = args(6)

    def getUserAttribution(arg: String) = {
      sc.textFile(arg)
        .map(_.split("\t"))
        .filter(_.length == 2)
        .filter(_(0).toInt >= REAL_USER_ID_BOUND)
    }

    getUserAttribution(args(0))
      .map(x => (x(0), "gender:" + x(1)))
      .saveAsTextFile(PREFIX + "user_attribution_gender")

    val userFollowingNumber = getUserAttribution(args(1))
      .map(x => (x(1), 1))
      .reduceByKey((a, b) => a + b)

    val userFollowerNumber = getUserAttribution(args(2))
      .map(x => (x(0), 1))
      .reduceByKey((a, b) => a + b)

    userFollowingNumber.join(userFollowerNumber)
      .map(x => {
      val user = x._1
      val followingRatio = (x._2._1 + 1.0) / (x._2._2 + 1.0) //
      (user, "followingRatio:" + followingRatio)
    })
      .saveAsTextFile(PREFIX + "user_attribution_following_ratio")

    getUserAttribution(args(3))
      .map(x => {
      val user = x(0)
      val birthday = x(1)
      val pattern = "([0-9]+)-([0-9][0-9])-([0-9][0-9])".r
      val age = birthday match {
        case pattern(birthdayYear, birthdayMonth, birthdayDay) => {
          val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
          val yearDiff = today.substring(0, 4).toInt - birthdayYear.toInt
          val delta = {
            if (today.substring(5, 10) > birthdayMonth + "-" + birthdayDay) 1
            else 0
          }
          yearDiff + delta
        }
        case _: String => -1
      }
      (user, "age:" + age.toString)
    }).saveAsTextFile(PREFIX + "user_attribution_age")

    getUserAttribution(args(4))
      .map(x => {
      (x(0), "oftenLocation:" + x(1))
    }).saveAsTextFile(PREFIX + "user_attribution_often_location")

    getUserAttribution(args(5))
      .map(x => (x(0), x(1)))
      .groupByKey()
      .map(x => {
      val user = x._1
      val tagCount = new mutable.HashMap[String, Int]()
      x._2.foreach(tag => {
        if (tagCount.get(tag).isEmpty)
          tagCount(tag) = 1
        else
          tagCount(tag) += 1
      })
      val mostTag = tagCount.toArray.sortWith(_._2 > _._2).take(3).map(_._1).sortWith(_ < _).mkString("|")
      (user, "mostTag:" + mostTag)
    }).saveAsTextFile(PREFIX + "user_attribution_most_tag")
  }
}
