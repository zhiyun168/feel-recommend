package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
import scala.util.Random._

/**
 * Created by canoe on 10/10/15.
 */
object RecommendSimilarUser {

  private val REAL_USER_ID_BOUND = 1075
  private var USER_NUMBER_UP_BOUND = 4000
  private val USER_NUMBER_BOTTOM_BOUND = 2
  private var CANDIDATES_SIZE = 200
  private val RDD_PARTITION_SIZE = 100
  private var COMMON_FOLLOWER_NUMBER = 5
  private val SAMPLE_THRESHOLD = 5000
  private var FOLLOWER_THRESHOLD = 500

  def knuthShuffle[T](x: Array[T]) = {
    for (i <- (1 until x.length).reverse) {
      val j = nextInt(i + 1)
      val tmp = x(i)
      x(i) = x(j)
      x(j) = tmp
    }
    x
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()

    USER_NUMBER_UP_BOUND = args(3).toInt
    FOLLOWER_THRESHOLD = args(4).toInt
    CANDIDATES_SIZE = args(5).toInt

    val sc = new SparkContext(conf)
    val followList = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)

    val followerNumber = followList
      .map(x => (x(0), 1))
      .reduceByKey(_ + _)
      .filter(_._2 <= FOLLOWER_THRESHOLD)

    val recentlyActiveUser = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(_(0))
      .filter(_.toInt >= REAL_USER_ID_BOUND)
      .distinct(RDD_PARTITION_SIZE)
      .map(x => (x, "_")) // recently active user

    COMMON_FOLLOWER_NUMBER = args(6).toInt

    val commonFollower = followList
      .map(x => (x(0), x(1))) // leader, follower
      .join(recentlyActiveUser) // r user
      .map(x => (x._1, x._2._1)) // rleader, follower
      .join(followerNumber)
      .map(x => (x._2._1, x._1)) // follower, rleader
      .groupByKey() //action
      .map(x => x._2.toArray)
      .filter(x => (x.length >= USER_NUMBER_BOTTOM_BOUND))
      .map(x => {
      if (x.length < SAMPLE_THRESHOLD) {
        x
      } else {
        knuthShuffle(x).take(SAMPLE_THRESHOLD)
      }
    })
      .flatMap(x => {
      val result = new Array[String](x.size * x.size)
      //case that user follows to many users and only one user just does not give a shot
      for (i <- 0 until x.length) {
        for (j <- 0 until x.length) {
          if (i != j)
            result(i * x.length + j) = x(i) + "\t" + x(j)
        }
      }
      result.toList.filter(_ != null)
    })
      .filter(x => x != "" && x != null)
      .map(x => (x, 1))
      .reduceByKey((a, b) => a + b)
      .filter(_._2 >= COMMON_FOLLOWER_NUMBER)
      .map(x => {
      val pair = x._1.split("\t")
      (pair(0), (x._2, pair(1)))
    }) // A, number, B
      .groupByKey()
      .map(x => {
      val recommend = x._2.toSeq.filter(_._2 != x._1).sortWith(_._1 > _._1).take(CANDIDATES_SIZE)
      (x._1, recommend)
    })

    commonFollower.map(x => (x._1 + "\t" + x._2.map(y => y._2 + ":" + y._1).mkString(","))).saveAsTextFile(args(0))

  }
}
