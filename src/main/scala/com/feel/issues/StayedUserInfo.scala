package com.feel.issues

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by canoe on 10/20/15.
 */
object StayedUserInfo {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val threeDaysAgoRegistered = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), "R"))

    val recentlyActiveUser = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), "S"))

    def userFeatureCount(rdd: RDD[String]) = {
      rdd.map(_.split("\t"))
        .filter(_.length == 2)
        .map(x => (x(0), 1))
        .reduceByKey((a, b) => a + b)
        .map(x => (x._1, x._2.toString))
    }

    val userFollowing = userFeatureCount(sc.textFile(args(2)))
    val userFollower = userFeatureCount(sc.textFile(args(3)))
    val userLikeNumber = userFeatureCount(sc.textFile(args(4)))
    val userCommentNumber = userFeatureCount(sc.textFile(args(5)))
    val userCardNumber = userFeatureCount(sc.textFile(args(6)))

    val stayedUser = threeDaysAgoRegistered.join(recentlyActiveUser)
      .map(x => (x._1, "S"))
    stayedUser.saveAsTextFile(args(7))

    def userData(aRDD: RDD[(String, String)], bRDD: RDD[(String, String)]) = {
      aRDD.leftOuterJoin(bRDD)
        .map(x => {
        x._2._2 match {
          case Some(number) => ((x._2._1, number), 1)
          case None => ((x._2._1, 0), 1)
        }
      })
        .reduceByKey((a, b) => a + b)
        .map(x => x._1._1 + "\t" + x._1._2 + "\t" + x._2)
    }

    userData(stayedUser, userFollowing).saveAsTextFile(args(8))
    userData(stayedUser, userFollower).saveAsTextFile(args(9))
    userData(stayedUser, userLikeNumber).saveAsTextFile(args(10))
    userData(stayedUser, userCommentNumber).saveAsTextFile(args(11))
    userData(stayedUser, userCardNumber).saveAsTextFile(args(12))

    val lostUser = threeDaysAgoRegistered.leftOuterJoin(recentlyActiveUser)
    .filter(x => {
      x._2._2 match {
        case Some(state) => false
        case None => true
      }
    })
    .map(x => (x._1, "L"))

    lostUser.saveAsTextFile(args(13))

    userData(lostUser, userFollowing).saveAsTextFile(args(14))
    userData(lostUser, userFollower).saveAsTextFile(args(15))
    userData(lostUser, userLikeNumber).saveAsTextFile(args(16))
    userData(lostUser, userCommentNumber).saveAsTextFile(args(17))
    userData(lostUser, userCardNumber).saveAsTextFile(args(18))
  }
}
