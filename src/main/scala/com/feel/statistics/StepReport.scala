package com.feel.statistics

import java.text.SimpleDateFormat
import java.util.Date

import breeze.linalg.min
import com.feel.utils.TimeIssues
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

import scala.util.Random._

/**
  * Created by canoe on 1/8/16.
  */
object StepReport {

  private val REAL_USER_ID_BOUND = 1075
  private val USER_SIZE_SAMPLE_BOUND = 10000

  def main(args: Array[String]) = {

    val sc = new SparkContext()

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val startTime = TimeIssues.nDaysAgoTs(args(7).toInt)
    val endTime = TimeIssues.nDaysAgoTs(args(8).toInt)

    val userStepNumber = mongoRDD.map(x => {
      val user = x._2.get("uid").toString
      val goalType = x._2.get("goal_type").toString
      val ts = x._2.get("record_time").toString.toLong / 1000
      val stepNumber = if (ts >= startTime && ts < endTime) {
        val step = try {
          goalType match {
            case "3" => {
              val deviceType = x._2.get("device")
              val stepScore = deviceType match {
                case "mi_band" =>
                  x._2.get("info").asInstanceOf[BSONObject].get("step").toString.toLong
                case _ =>
                  x._2.get("info").asInstanceOf[BSONObject].get("sum").asInstanceOf[BSONObject].get("step")
                    .toString.toLong
              }
              stepScore
            }
            case _ => 0
          }
        } catch {
          case _: Throwable => 0
        }
        step
      } else {
        0
      }
      (user, stepNumber)
    }).filter(_._2 != 0)
      .reduceByKey((a, b) => a + b)

    userStepNumber.map(x => x._1 + "\t" + "total_step:" + x._2.toString).saveAsTextFile(args(6))

    val userFollowing = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2) //leader, follower
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1)))

    val rankInFollowingUserStep = userFollowing.join(userStepNumber)
    .map({
      case (leader, (follower, stepNumber)) => {
        (follower, stepNumber)
      }
    }).groupByKey()
      .join(userStepNumber)
    .map({case (leader, (followerStepNumber, leaderStepNumber)) => {
      val user = leader
      val rankedStepNumber = followerStepNumber.toArray.sortWith(_ < _)

      var left = 0
      var right = rankedStepNumber.length - 1
      var index = rankedStepNumber.length

      while (left <= right) {
        val middle = (left + right) >> 1
        if (leaderStepNumber <= rankedStepNumber(middle)) {
          index = middle
          right = middle - 1
        } else {
          left = middle + 1
        }
      }
      val rank = min(index + 1, rankedStepNumber.length)
      user + "\tranking_in_following:" + rank.toString
    }
    })
    rankInFollowingUserStep.saveAsTextFile(args(3))

    val userInfo = sc.textFile(args(4))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .map(x => {
        val user = x(0)
        /*val gender = x(1)
        val birthday = x(2)
        val pattern = "([0-9]+)-([0-9][0-9])-([0-9][0-9])".r
        val age = birthday match {
          case pattern(birthdayYear, birthdayMonth, birthdayDay) => {
            val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            val yearDiff = today.substring(0, 4).toInt - birthdayYear.toInt
            val delta = {
              if (today.substring(5, 10) > birthdayMonth + "-" + birthdayDay) 1
              else -1
            }
            if (yearDiff > 100 || yearDiff < 3) 0 else yearDiff + delta
          }
          case _ => 0
        }
        if (gender == "x" || age == 0) {
          (user, "default")
        } else {
          (user, gender + "," + age.toString)
        }*/
        (user, "default")
      })

    def knuthShuffle[T](x: Array[T]) = {
      for (i <- (1 until x.length).reverse) {
        val j = nextInt(i + 1)
        val tmp = x(j)
        x(j) = x(i)
        x(i) = tmp
      }
      x
    }

    val specifiedUserStepArray = userInfo.join(userStepNumber)
      .map(x => x._2)
      .groupByKey()
      .map(x => {
        val key = x._1
        val stepArray = if (x._2.size < USER_SIZE_SAMPLE_BOUND) {
          x._2.toArray.sortWith(_ < _)
        } else {
          knuthShuffle(x._2.toArray).take(USER_SIZE_SAMPLE_BOUND).sortWith(_ < _)
        }
        (key, stepArray)
      })
    specifiedUserStepArray.saveAsTextFile(args(9))

    userInfo.join(userStepNumber)
      .map(x => (x._2._1, (x._1, x._2._2))) //(info, (user, stepNumber))
      .join(specifiedUserStepArray) //info, stepArray
      .map({case (info, ((user, stepNumber), stepArray)) => {
      val size = stepArray.size
      var left = 0
      var right = size - 1
      var index = size

      while(left <= right) {
        val middle = (left + right) >> 1
        if (stepNumber <= stepArray(middle)) {
          index = middle
          right = middle - 1
        } else {
          left = middle + 1
        }
      }
      user + "\t" + "user_step_rank_ratio: [" + info + "," +
        (index.toDouble * 100 / size).formatted("%.1f") + "%]"
    }}).saveAsTextFile(args(5))
  }
}
