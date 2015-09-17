package com.feel.recommend

import breeze.linalg.min
import scala.util.Random.nextInt
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by canoe on 9/17/15.
 */
object RecommendCardBasedOnGoalJoined {

  private val REAL_USER_ID_BOUND = 1075
  private val GOAL_USER_NUMBER_THRESHOLD = 2000
  private var CARD_LIKED_THRESHOLD = 3
  private val CANDIDATES_SIZE = 100

  def knuthShuffle[T](x: Array[T]) = {
    for (i <- (1 until x.length).reverse) {
      val j = nextInt(i + 1)
      val tmp = x(j)
      x(j) = x(i)
      x(i) = tmp
    }
    x
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val userGoal = sc.textFile(args(0)) // user, goal
      .map(_.split("\t"))
      .filter(x => x.length == 2 && x(0).toInt >= REAL_USER_ID_BOUND)

    val goalUserRecommend = userGoal.map(x => (x(1), x(0)))
    .groupByKey()
    .flatMap(x => {
      val goalUserNumber = x._2.size
      val goalUsers = x._2.toArray
      if (x._2.size >= GOAL_USER_NUMBER_THRESHOLD) {
        val itNumber = min(goalUserNumber / GOAL_USER_NUMBER_THRESHOLD, 20)
        val result = new Array[(String, String)](itNumber * GOAL_USER_NUMBER_THRESHOLD * GOAL_USER_NUMBER_THRESHOLD)
        for(it <- 0 until itNumber) {
          val sampledUsers = knuthShuffle(goalUsers).take(GOAL_USER_NUMBER_THRESHOLD)
          for(i <- 0 until GOAL_USER_NUMBER_THRESHOLD) {
            for(j <- 0 until GOAL_USER_NUMBER_THRESHOLD) {
              if (i != j)
                result(it * GOAL_USER_NUMBER_THRESHOLD * GOAL_USER_NUMBER_THRESHOLD + i * GOAL_USER_NUMBER_THRESHOLD +
                  j) = (sampledUsers(i), sampledUsers(j))
            }
          }
        }
        result.filter(_ != null).distinct.toSeq
      } else {
        val result = new Array[(String, String)](goalUserNumber * goalUserNumber)
        for(i <- 0 until goalUserNumber) {
          for(j <- 0 until goalUserNumber) {
            if (i != j)
              result(i * goalUserNumber + j) = (goalUsers(i), goalUsers(j))
          }
        }
        result.filter(_ != null).toSeq
      }
    })

    val userGender = sc.textFile(args(1)) // user, gender
      .map(_.split("\t"))
      .filter(x => x.length == 2 && x(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1)))

    CARD_LIKED_THRESHOLD = args(5).toInt

    val cardLikedNumber = sc.textFile(args(2))
      .map(_.split("\t")) //user, card
      .filter(x => x.length == 2)
      .map(x => (x(1), 1))
      .reduceByKey((a, b) => a + b)
      .filter(_._2 >= CARD_LIKED_THRESHOLD)

    val userCard = sc.textFile(args(3)) // user, card, type
      .map(_.split("\t"))
      .filter(x => x.length == 3 && x(0).toInt >= REAL_USER_ID_BOUND && x(2) == "card")
      .map(x => (x(1), x(0))) // card, user
      .join(cardLikedNumber) // filter
      .map(x => (x._2._1, x._1)) //user, card

    val userFollowingSet = sc.textFile(args(4))
      .map(_.split("\t"))
      .filter(x => x.length == 2 && x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(1), x(0)))
      .groupByKey()
      .map(x => (x._1, x._2.toSet))

    val result = goalUserRecommend.join(userGender)
    .map(x => {
      (x._2._1, (x._1, x._2._2)) //recommended, (user, gender)
    }).join(userGender) // recommended, ((user, gender), recommendedGender)
      .filter(x => x._2._1._2 != x._2._2)
      .map(x => (x._1, x._2._1._1)) //user, recommended
      .leftOuterJoin(userFollowingSet) //user, (recommended, followingSet)
      .filter(x => {
        x._2._2 match {
          case Some(followingSet) => !followingSet(x._1)
          case None => true
        }
    }).map(x => (x._2._1, x._1)) //recommended, user
      .join(userCard) // recommended, (user, Card)
      .map(_._2)
      .groupByKey()
      .map(x => {
      val user = x._1
      val cardCandidates = x._2.take(CANDIDATES_SIZE)
      (user, cardCandidates)
    })
    result.saveAsTextFile(args(6))
  }
}
