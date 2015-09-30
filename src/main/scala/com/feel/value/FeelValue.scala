package com.feel.value

import breeze.linalg.min
import breeze.numerics.{log2, floor}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BSONObject
import org.apache.hadoop.conf.Configuration
import org.elasticsearch.spark._

/**
 * Created by canoe on 9/28/15.
 */

case class FeelValue(user: String, value: String)

object FeelValue {

  private val REAL_USER_ID_BOUND = 1075

  private val featureMap = List(
    -1 -> "historyTotalScore",
    0 -> "everyDayLikeNumber",
    1 -> "everyDayLikedNumber",
    2 -> "everyDayCardNumber",
    3 -> "everyDayGoalNumber",
    4 -> "everyDayFollowNumber",
    5 -> "everyDayFollowedNumber",
    6 -> "everyDayCommentNumber",
    7 -> "everyDayCommentedNumber",
    8 -> "everyDaySportsScore").toMap

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    val yesterdayFeelScoreRDD = sql.read.format("org.elasticsearch.spark.sql").load(args(1))
    .map(x => (x(0).toString + "\t-1:" + x(1).toString))

    val fakedLikeRDD = sc.textFile(args(11))
    .map(x => (x, 0))

    val likeRDD = sc.textFile(args(2)) // user like
      .map(x => (x, 1))
      .leftOuterJoin(fakedLikeRDD)
      .filter(x => {
      x._2._2 match {
        case Some(number) => false
        case None => true
      }
    }).map(_._1)
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND)

    def userRDDTransform(rdd: RDD[Array[String]], index: Int, featureIndex: Int) = {
      rdd
        .map(x => (x(index), 1))
        .reduceByKey((a, b) => a + b)
        .map(x => (x._1 + "\t" + featureIndex + ":" + min(10, floor(log2(x._2.toDouble)).toInt).toString))
    }
    val userLikeNumber = userRDDTransform(likeRDD, 0, 0)


    val cardRDD = sc.textFile(args(3)) // user, card, type
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND)

    def cardRDDTransform(rdd: RDD[Array[String]], cardType: String, featureIndex: Int) = {
      val cardValue = if (cardType == "card") 2 else 1
      rdd
        .filter(x => x(2) == cardType)
        .map(x => (x(0), 1))
        .reduceByKey((a, b) => a + b)
        .map(x => (x._1 + "\t" + featureIndex + ":" + min(10, x._2 * cardValue).toString))
    }

    val userCardNumber = cardRDDTransform(cardRDD, "card", 2)
    val userGoalNumber = cardRDDTransform(cardRDD, "goal", 3)

    val fakedFollowRDD = sc.textFile(args(12))
    .map(x => (x, 0))

    val followRDD = sc.textFile(args(4))
      .map(x => (x, 1))
      .leftOuterJoin(fakedFollowRDD)
      .filter(x => {
      x._2._2 match {
        case Some(number) => false
        case None => true
      }
    }).map(_._1)
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)

    val userFollowingNumber = userRDDTransform(followRDD, 1, 4)
    val userFollowedNumber = userRDDTransform(followRDD, 0, 5)


    val cardOwner = cardRDD.map(x => (x(1), x(0)))
    val likedRDD = likeRDD.map(x => (x(1), x(0))) // like, user
      .join(cardOwner) // card, (user, owner)
      .map(x => Array(x._2._2)) // owner, card

    val userLikedNumber = userRDDTransform(likedRDD, 0, 1)

    val commentRDD = sc.textFile(args(5))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0) != x(1) && x(0) != "None" && x(1) != "None" && x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt
      >= REAL_USER_ID_BOUND)

    val userCommentNumber = userRDDTransform(commentRDD, 0, 6)
    val userCommentedNumber = userRDDTransform(commentRDD, 1, 7)

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(6))
    hadoopConf.set("mongo.input.uri", args(7))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userSportsScore = mongoRDD.map(x => {
      val user = x._2.get("uid")
      val goalType = x._2.get("goal_type").toString
      val ts = x._2.get("record_time").toString.toDouble
      val sportsScore = if (ts < args(8).toDouble) {
        0
      } else {
        val deviceScore = try {
          goalType match {
            case "3" => {
              val deviceType = x._2.get("device")
              val stepScore = deviceType match {
                case "mi_band" =>
                  val step = x._2.get("info").asInstanceOf[BSONObject].get("step").toString.toDouble
                  min(10, (step / 2000).toInt + 3)
                case _ =>
                  val step = x._2.get("info").asInstanceOf[BSONObject].get("sum").asInstanceOf[BSONObject].get("step")
                  .toString.toDouble
                  min(10, floor(step / 2000).toInt)
              }
              stepScore
            }
            case "4" => 3
            case "5" => if (x._2.get("device") != "mi_band") 0 else 3
            case "6" => {
              val distance = x._2.get("info").asInstanceOf[BSONObject].get("distance").toString.toDouble
              min(10, (distance / 1000).toInt)
            }
          }
        } catch {
          case _ => 0
        }
        deviceScore
      }
      user + "\t8:" + min(15, sportsScore)
    })

    val userFeelDetailScore = userSportsScore.union(userLikeNumber).union(userLikedNumber).union(userCardNumber)
      .union(userGoalNumber).union(userFollowingNumber).union(userFollowedNumber).union(userCommentedNumber)
      .union(userCommentNumber).union(userSportsScore).union(yesterdayFeelScoreRDD)
      .map(_.split("\t"))
      .map(x => (x(0), x(1)))
      .groupByKey()
      .map(x => {
      val user = x._1
      val (socialScore, sportsScore, previousScore) = x._2.toSeq.foldLeft((0, 0, 0))((score, valueInfo) => {
        val valueTmp = valueInfo.split(":")
        if (valueTmp(0).toInt == -1)
          (score._1, score._2, score._3 + valueTmp(1).toInt)
        else if (valueTmp(0).toInt <= 7 && valueTmp(0).toInt >= 0)
          (min(10, score._1 + valueTmp(1).toInt), score._2, score._3)
        else
          (score._1, min(15, score._2 + valueTmp(1).toInt), score._3)
      })
      (user, socialScore, sportsScore, socialScore + sportsScore, previousScore,
        socialScore + sportsScore + previousScore)
    })
    userFeelDetailScore.saveAsTextFile(args(9))
    userFeelDetailScore.map(x => FeelValue(x._1, x._6.toString)).saveToEs(args(1))
  }
}
