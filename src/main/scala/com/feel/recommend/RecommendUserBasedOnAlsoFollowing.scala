package com.feel.recommend

/**
 * Created by canoe on 6/19/15.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import scala.collection.immutable.HashSet
import scala.util.Random.nextInt
import scala.util.parsing.json.JSON

case class AlsoFlowingUserRecommend(user: String, candidates: Seq[String])

object RecommendUserBasedOnAlsoFollowing {

  private val REAL_USER_ID_BOUND = 1075
  private var USER_NUMBER_UP_BOUND = 4000
  private val USER_NUMBER_BOTTOM_BOUND = 2
  private var CANDIDATES_SIZE = 200
  private val RDD_PARTITION_SIZE = 100
  private var COMMON_FOLLOWER_NUMBER = 5
  private val SAMPLE_THRESHOLD = 2000
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
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))

    USER_NUMBER_UP_BOUND = args(4).toInt
    FOLLOWER_THRESHOLD = args(6).toInt
    CANDIDATES_SIZE = args(7).toInt

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

    COMMON_FOLLOWER_NUMBER = args(5).toInt

    val commonFollower = followList
      .map(x => (x(0), x(1))) // leader, follower
      .join(recentlyActiveUser) // r user
      .map(x => (x._1, x._2._1)) // rleader, follower
      .join(followerNumber)
      .map(x => (x._2._1, x._1)) // follower, rleader
      .reduceByKey((a, b) => a + "\t" + b) //action
      .map(x => x._2.split("\t"))
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
      val recommend = x._2.toSeq.sortWith(_._1 > _._1).take(CANDIDATES_SIZE)
      (x._1, recommend)
    })

    val following = followList
      .map(x => (x(0), x(1)))

    val userDislikeSet = sc.textFile(args(8))
      .map(x => {
      val xMap = JSON.parseFull(x)
      xMap match {
        case Some(map: Map[String, Any]) => {
          if (map("action") == "dislike")
            map("id") + "\t" + map("candidates").toString().replaceAll("[\\[\\]A-Za-z()]", "")
          else "?"
        }
        case _ => "?"
      }
    }).filter(_ != "?")
      .map(_.split("\t"))
      .map(x => (x(0), x(1)))
      .groupByKey()
      .map(x => {
      (x._1, x._2.toSet)
    })

    val result = commonFollower.join(following, RDD_PARTITION_SIZE) // following, followingRecommend, user
      .map(x => (x._2._2, (x._1, x._2._1))) // user, following, followingRecommend
      .groupByKey()
      .leftOuterJoin(userDislikeSet) // user, dislikeSet
      .map(x => {// user, ((following, followingRecommend), dislikeSet)
      val user = x._1
      val dislikeSet = x._2._2 match {
        case Some(set) => set
        case None => new HashSet[String]()
      }
      val value = x._2._1.toSeq
      val followSet = value.map(_._1).toSet
      val candidates = value.map(_._2).flatten.filter(x => !dislikeSet(x._2) && !followSet(x._2) && x._2 != user)
        .sortWith(_
        ._1 > _._1)
        .map(_._2).distinct.take(CANDIDATES_SIZE)
      (user, candidates)
    })

    /*result
    .map(x => AlsoFlowingUserRecommend(x._1, x._2))
    .saveToEs("recommendation/alsoFollowing")*/
    result
      .map(x => (x._1, x._2))
      .saveAsTextFile(args(3))
  }
}
