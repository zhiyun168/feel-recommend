package com.feel.recommend


import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ArrayBuffer, HashMap}
import org.elasticsearch.spark._
import scala.util.Random.nextInt

/**
 * Created by canoe on 7/15/15.
 */

case class CardRecommend(user: String, candidates: Seq[String])

object RecommendCardBasedOnFollowingUserLiked {


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

    val card = sc.textFile(args(2))
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
      .join(card) //likedCard, user, "_"
      .map(x => (x._2._1, (x._1, x._2._1))) //user, likedCard
      .groupByKey()

    val recentlyActiveUser = sc.textFile(args(8))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))

    val followingLikedCard = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1))) // following, user
      .join(userLikedCard) // following, user, liked card
      .map(x => (x._2._1, x._2._2)) // user, {(recommended card, whoLiked)}
      .join(recentlyActiveUser)
      .map(x => (x._1, x._2._1))
      .groupByKey()
      .leftOuterJoin(userLikedCard) // user, {{(recommended card, whoLiked)}}, (userLikedCard, whoLiked)
      .flatMap(x => {
      val user = x._1
      val cardLikedCount = new HashMap[String, Int]
      val followingLikedCard = x._2._1.flatten
      followingLikedCard.foreach(card => {
        if (cardLikedCount.get(card._1).isEmpty) {
          cardLikedCount(card._1) = 1
        } else {
          cardLikedCount(card._1) += 1
        }
      })

      val cardLikedUser = new HashMap[String, ArrayBuffer[String]]
      followingLikedCard.foreach(card => {
        if (cardLikedUser.get(card._1).isEmpty) {
          val arrayBufferTmp = new ArrayBuffer[String]()
          arrayBufferTmp.append(card._2)
          cardLikedUser(card._1) = arrayBufferTmp
        } else {
          cardLikedUser(card._1).append(card._2)
        }
      })

      val candidateList = x._2._2 match {
        case Some(userLikedCardList) => {
          val userLikedCardSet = userLikedCardList.map(_._1).toSet
          cardLikedCount.filter(_._2 <= FOLLOWING_LIKED_UPPER_BOUND)
            .filter(x => !userLikedCardSet(x._1))
            .map(x => {
            val randomIndex = nextInt(cardLikedUser(x._1).length)
            (x._1, (user, cardLikedUser(x._1)(randomIndex)))
          }) //recommendCard, rUser
        }
        case None => {
          cardLikedCount.filter(_._2 <= FOLLOWING_LIKED_UPPER_BOUND)
            .map(x => {
            val randomIndex = nextInt(cardLikedUser(x._1).length)
            (x._1, (user, cardLikedUser(x._1)(randomIndex)))
          }) //recommendCard, rUser
        }
      }
      candidateList
    })

    val followingSetRDD = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(1), x(0)))
      .groupByKey()
      .map(x => (x._1, x._2.toSet)) //user, followingSet

    val cardOwner = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(1), x(0))) //card, owner

    val filteredFollowingLikedCard = followingLikedCard //recommendedCard, (rUser, whoLike)
      .join(cardOwner) // recommendedCard, ((rUser, whoLike), owner)
      .map(x => (x._2._1._1, (x._1, x._2._2, x._2._1._2))) //rUser, (recommendedCard, owner, whoLike)
      .leftOuterJoin(followingSetRDD) // rUser, ((recommendedCard, owner, whoLike), followingSet)
      .filter(x => {
      x._2._2 match {
        case Some(userFollowingSet) => {
          !userFollowingSet(x._2._1._2)
        }
        case None => true
      }
    }).map(x => (x._2._1._1, (x._1, x._2._1._3))) //recommendedCard, (rUser, whoLike)

    val cardLikedNumber = sc.textFile(args(3))
      .distinct(RDD_PARTITION_NUMBER)
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(1), 1))
      .reduceByKey((a, b) => a + b)

    val result = filteredFollowingLikedCard
      .join(cardLikedNumber)// recommended card, (user, whoLike), cardLikedNumber
      .map(x => (x._2._1._1, (x._1, x._2._2, x._2._1._2))) //user, (recommended card, cardLikedNumber, whoLiked)
      .groupByKey()
      .map(x => {
      val user = x._1
      val candidates = x._2.toSeq.sortWith(_._2 > _._2).map(y => y._1 + ":" + y._3).distinct.take(CANDIDATE_SIZE)
      CardRecommend(user, candidates)
    })
    result.saveToEs(args(7) + "/CARD")
    result.saveAsTextFile(args(4))
  }
}
