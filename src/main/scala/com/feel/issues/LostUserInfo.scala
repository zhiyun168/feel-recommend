package com.feel.issues

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BSONObject

/**
 * Created by canoe on 10/17/15.
 */


object LostUserInfo {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val lostUser = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))
      .groupByKey()
      .filter(x => {
      val longTs = x._2.toSeq.sortWith(_.toLong > _.toLong)
      val dateFormat = new SimpleDateFormat("ss")
      if (longTs.length == 1) {
        val smallestTime = dateFormat.parse(longTs.head).getTime / 1000
        val biggestTime = System.currentTimeMillis() / 1000
        if (biggestTime - smallestTime > 5 * 3600 * 24) true else false
      }
      else {
        val smallestTime = dateFormat.parse(longTs.head).getTime / 1000
        val biggestTime = dateFormat.parse(longTs.last).getTime / 1000
        if (biggestTime - smallestTime > 5 * 3600 * 24) false else true
      }
    }).map(x => (x._1, "_"))




    val userGender = sc.textFile(args(1))
      .map(_.split(("\t")))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))

    val userAge = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1).substring(0, 4)))

    def userFeatureCount(rdd: RDD[String]) = {
      rdd.map(_.split("\t"))
        .filter(_.length == 2)
        .map(x => (x(0), 1))
        .reduceByKey((a, b) => a + b)
        .map(x => (x._1, x._2.toString))
    }

    val userFollowing = userFeatureCount(sc.textFile(args(3)))
    val userLikeNumber = userFeatureCount(sc.textFile(args(4)))
    val userCommentNumber = userFeatureCount(sc.textFile(args(5)))
    val userCardNumber = userFeatureCount(sc.textFile(args(6)))

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(7))
    hadoopConf.set("mongo.input.uri", args(8))
    val deviceRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userResolution = deviceRDD.map(x => {
      val user = x._2.get("uid").toString
      val resolution = x._2.get("resolution").toString
      (user, resolution)
    }).distinct()

    val userModel = deviceRDD.map(x => {
      val user = x._2.get("uid").toString
      val model = x._2.get("phoneModel").toString
      (user, model)
    }).distinct()

    def LostUserData(aRDD: RDD[(String, String)], bRDD: RDD[(String, String)]) = {
      aRDD.join(bRDD)
        .map(x => (x._2._2, 1))
        .reduceByKey((a, b) => a + b)
        .map(x => x._1 + "\t" + x._2)
    }

    LostUserData(lostUser, userGender).saveAsTextFile(args(9))
    LostUserData(lostUser, userAge).saveAsTextFile(args(10))
    LostUserData(lostUser, userFollowing).saveAsTextFile(args(11))
    LostUserData(lostUser, userLikeNumber).saveAsTextFile(args(12))
    LostUserData(lostUser, userCommentNumber).saveAsTextFile(args(13))
    LostUserData(lostUser, userCardNumber).saveAsTextFile(args(14))
  }
}
