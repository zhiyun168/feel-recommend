package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.HashMap
import org.elasticsearch.spark._

/**
 * Created by canoe on 7/15/15.
 */

case class cardRecommend(user: String, candidates: Seq[String])

object recommendCardBasedOnFollowingUserLiked {


  private val REAL_USER_ID_BOUND = 1075
  private val RDD_PARTITION_NUMBER = 10
  private val CANDIDATE_SIZE = 100
  private var FOLLOWING_LIKED_UPPER_BOUND = 100
  private var FOLLOWER_NUMBER_UPPER_BOUND = 1000

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))

    val sc = new SparkContext(conf)

    FOLLOWING_LIKED_UPPER_BOUND = args(5).toInt
    FOLLOWER_NUMBER_UPPER_BOUND = args(6).toInt

    val userFollowerNumber = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= RDD_PARTITION_NUMBER)
      .map(x => (x(0), 1))
      .reduceByKey((a, b) => a + b)

    val userCard = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND) //user, card
      .map(x => (x(0), x(1)))
      .join(userFollowerNumber)
      .filter(x => x._2._2 <= FOLLOWER_NUMBER_UPPER_BOUND)
      .map(x => (x._2._1, "_")) //card

    val userLikedCard = sc.textFile(args(3))
      .distinct(RDD_PARTITION_NUMBER)
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(1), x(0))) //likedCard, user
      .join(userCard)
      .map(x => (x._2._1, x._1)) //user, likedCard
      .groupByKey()

    val followingLikedCard = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1))) // following, user
      .join(userLikedCard)
      .map(x => (x._2._1, x._2._2))
      .groupByKey()
      .join(userLikedCard)
      .flatMap(x => {
      val user = x._1
      val followingLikedCard = x._2._1.flatten
      val userLikedCardSet = x._2._2.toSet
      val hashMap = new HashMap[String, Int]
      followingLikedCard.foreach(card => {
        if (hashMap.get(card).isEmpty) {
         hashMap(card) = 1
        } else {
          hashMap(card) += 1
        }
      })
      hashMap.toSeq.filter(_._2 <= FOLLOWING_LIKED_UPPER_BOUND)
        .filter(x => !userLikedCardSet(x._1))
        .sortWith(_._2 > _._2)
        .map(x => (x._1, user)) //recommendCard, User
    })

    val cardLikedNumber = sc.textFile(args(3))
      .distinct(RDD_PARTITION_NUMBER)
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(1), 1))
      .reduceByKey((a, b) => a + b)

    val result = followingLikedCard
      .join(cardLikedNumber)
      .map(x => (x._2._1, (x._1, x._2._2))) //user, card, number
      .groupByKey()
      .map(x => {
      val user = x._1
      val candidates = x._2.toSeq.sortWith(_._2 > _._2).map(_._1).take(CANDIDATE_SIZE)
      cardRecommend(user, candidates)
    })
    result.saveToEs("recommendation/followingUserLikedCard")
    result.saveAsTextFile(args(4))
  }
}
