package com.feel.recommend

/**
 * Created by canoe on 6/19/15.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

case class alsoFlowingUserRecommend(user: String, candidates: Seq[String])

object recommendUserBasedOnAlsoFollowing {

  private val REAL_USER_ID_BOUND = 1075
  private var USER_NUMBER_UP_BOUND = 4000
  private val USER_NUMBER_BOTTOM_BOUND = 2
  private val CANDIDATES_SIZE = 100
  private val RDD_PARTITION_SIZE = 100
  private var COMMON_FOLLOWER_NUMBER = 10

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))

    USER_NUMBER_UP_BOUND = args(3).toInt

    val sc = new SparkContext(conf)
    val followList = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)

    COMMON_FOLLOWER_NUMBER = args(4).toInt

    val commonFollower = followList
      .map(x => (x(1), x(0)))
      .reduceByKey((a , b) => a + "\t" + b) //action
      .map(x => x._2.split("\t"))
      .filter(x => (x.length >= USER_NUMBER_BOTTOM_BOUND && x.length <= USER_NUMBER_UP_BOUND))
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
      val recommend = x._2.toSeq.sortWith(_._1 > _._1)
      (x._1, recommend)
    })

    val filteredFollowList =
      followList.map(x => (x(1), x(0)))
        .reduceByKey((a, b) => a + "\t" + b)
        .map(x => (x._1, x._2.split("\t")))
        .filter(x => x._2.length <= USER_NUMBER_UP_BOUND)
        .flatMap(x => x._2.map(y => (y, x._1))) // followed, user

    val result = commonFollower.join(filteredFollowList, RDD_PARTITION_SIZE) // followed, recommend, user
      .map(x => (x._2._2, (x._1, x._2._1))) // user, following, recommend
      .groupByKey()
      .map(x => {
      val user = x._1
      val value = x._2.toSeq
      val followSet = value.map(_._1).toSet
      val candidates = value.map(_._2).flatten.filter(x => !followSet(x._2) && x._2 != user)
        .sortWith(_._1 > _._1)//todo recount when memory get bigger
        .map(_._2)
        .distinct
        .take(CANDIDATES_SIZE)
      (user, candidates)
    })
    result
      .map(x => alsoFlowingUserRecommend(x._1, x._2))
      .saveToEs("recommendation/alsoFollowing")
    result
      .map(x => (x._1, x._2))
      .saveAsTextFile(args(2))
  }
}
