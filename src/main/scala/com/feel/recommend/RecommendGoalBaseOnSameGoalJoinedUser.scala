package com.feel.recommend

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by canoe on 10/27/15.
 */
object RecommendGoalBaseOnSameGoalJoinedUser {

  private val GOAL_USER_NUMBER_LOWER_BOUND = 10
  private val GOAL_USER_NUMBER_UP_BOUND = 800
  private val CANDIDATES_SIZE = 100

  case class GoalRecommend(user: String, candidates: Seq[String])

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))
    val sc = new SparkContext(conf)

    val userGoal = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1))) //user, goal

    val goalUserNumber = userGoal
      .map(x => (x._2, 1))
      .reduceByKey((a, b) => a + b)

    val filteredUserGoal = userGoal
      .map(x => (x._2, x._1)) // goal, user
      .join(goalUserNumber)
      .filter(_._2._2 >= GOAL_USER_NUMBER_LOWER_BOUND)
      .map(x => (x._2._1, x._1)) //user, goal
      .groupByKey()
      .map(x => (x._1, x._2.toSet)) //user, joinedUserSet

    val recommendedGoal = userGoal
      .map(x => (x._2, x._1))
      .groupByKey() //goal, userList
      .filter(x => x._2.size >= GOAL_USER_NUMBER_LOWER_BOUND && x._2.size <= GOAL_USER_NUMBER_UP_BOUND)
      .flatMap(x => {
      val goalUserList = x._2.toSeq
      val goalUserSize = x._2.size
      val goalUserPairList = new ArrayBuffer[(String, String)]()
      for (i <- 0 until goalUserSize) {
        for (j <- 0 until goalUserSize) {
          if (i != j) {
            goalUserPairList.append((goalUserList(i), goalUserList(j)))
          }
        }
      }
      goalUserPairList.toSeq
    }).map(x => (x, 1))
      .reduceByKey((a, b) => a + b)
      .filter(_._2 >= 2)
      .map(x => (x._1._1, (x._1._2, x._2))) //(aUser, (bUser, number))
      .join(userGoal) //user, ((commonGoalUser, number), userGoal)
      .map(x => (x._2._1._1, (x._2._2, x._1, x._2._1._2))) //user, (recommendGoal, specifiedUser, number)
      .groupByKey() //user, (recommendGoalList, specifiedUser, number)
      .join(filteredUserGoal) //user, ((recommendGoalList, specifiedUser, number), joinedGoalSet)
      .map(x => {
      val user = x._1
      val joinedGoalSet = x._2._2
      val recommendedGoalList = x._2._1.filter(y => !joinedGoalSet(y._1)).toSeq
        .map(y => y._1 + ":" + y._2 + ":" + y._3)
        .distinct
        .take(CANDIDATES_SIZE)
      (user, recommendedGoalList)
    })
    recommendedGoal.saveAsTextFile(args(2))
    recommendedGoal.map(x => GoalRecommend(x._1, x._2)).saveToEs("recommendation/goalCommonUserGoal")
  }
}
