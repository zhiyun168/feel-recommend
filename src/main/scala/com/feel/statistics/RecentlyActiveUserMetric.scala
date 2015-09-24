package com.feel.statistics

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by canoe on 8/28/15.
 */
object RecentlyActiveUserMetric {

  private val REAL_USER_ID_BOUND = 1075

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val recentlyActiveUser = sc.textFile(args(0))
      .map(x => x.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), "_"))

    val userGender = sc.textFile(args(1))
      .map(x => x.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1))) //user, gender

    val recentlyActiveUserGender = recentlyActiveUser
      .join(userGender)
      .map(x => (x._1, x._2._2)) //recently active user, gender

    val recentlyUserLikedCard = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND) //user, likedCard
      .map(x => (x(0), 1)) // user
      .reduceByKey((a, b) => a + b)
      .join(recentlyActiveUserGender)
      .map(x => (x._2, 1))
      .reduceByKey((a, b) => a + b)

    recentlyUserLikedCard
      .map(x => x._1._2 + "\t" + x._1._1 + "\t" + x._2)
      .saveAsTextFile(args(5))

    val recentlyUserCard = sc.textFile(args(3)) // user, card, type
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(2) != "goal")
      .map(x => (x(0), 1))
      .reduceByKey((a, b) => a + b)
      .join(recentlyActiveUserGender)//user, (number, gender)
      .map(x => (x._2, 1))
      .reduceByKey((a, b) => a + b)

    recentlyUserCard
      .map(x => x._1._2 + "\t" + x._1._1 + "\t" + x._2)
      .saveAsTextFile(args(6))

    val recentlyUserGoal = sc.textFile(args(3)) // user, goal, type
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(2) == "goal")
      .map(x => (x(0), 1))
      .reduceByKey((a, b) => a + b)
      .join(recentlyActiveUserGender)
      .map(x => (x._2, 1))
      .reduceByKey((a, b) => a + b)

    recentlyUserGoal
      .map(x => x._1._2 + "\t" + x._1._1 + "\t" + x._2)
      .saveAsTextFile(args(7))

    val cardOwner = sc.textFile(args(3)) // owner, card, type
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(1), x(0))) // card, owner

    val recentlyUserLike = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(1), 1))
      .reduceByKey((a, b) => a + b)
      .join(cardOwner) // card, number, owner
      .map(x => (x._2._2, x._2._1)) // owner, number
      .join(recentlyActiveUserGender) // owner, number,  gender
      .map(x => (x._2, 1))
      .reduceByKey((a, b) => a + b)
    recentlyUserLike
      .map(x => x._1._2 + "\t" + x._1._1 + "\t" + x._2)
      .saveAsTextFile(args(8))

    val followRDD = sc.textFile(args(4))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)

    val followerNumber = followRDD
      .map(x => (x(0), 1))
      .reduceByKey((a, b) => a + b) // user, followerNumber
      .join(recentlyActiveUserGender) // user gender followerNumber
      .map(x => (x._2, 1))
      .reduceByKey((a, b) => a + b)

    followerNumber
      .map(x => x._1._2 + "\t" + x._1._1 + "\t" + x._2)
      .saveAsTextFile(args(9))

    val followingNumber = followRDD
      .map(x => (x(1), 1))
      .reduceByKey((a, b) => a + b)
      .join(recentlyActiveUserGender)
      .map(x => (x._2, 1))
      .reduceByKey((a, b) => a + b)

    followingNumber
      .map(x => x._1._2 + "\t" + x._1._1 + "\t" + x._2)
      .saveAsTextFile(args(10))

  }
}
