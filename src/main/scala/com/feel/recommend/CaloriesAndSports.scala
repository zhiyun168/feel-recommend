package com.feel.recommend

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 12/10/15.
  */
object CaloriesAndSports {

  def getTodayBeginEndTs() = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    val startTime = dateFormat.parse(dateFormat.format(calendar.getTime)).getTime / 1000
    calendar.add(Calendar.DATE, 1)
    val endTime = dateFormat.parse(dateFormat.format(calendar.getTime)).getTime / 1000
    (startTime, endTime)
  }

  def main(args: Array[String]) = {

    val sc = new SparkContext()
    val hadoopConf = new Configuration()

    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))

    val dataRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val (startTime, endTime) = getTodayBeginEndTs()

    val userIntakeCalories = dataRDD.filter(x => x._2.get("goal_type").toString.equalsIgnoreCase("8"))
      .map(x => {
        val user = x._2.get("uid").toString
        val caloriesInfo = x._2.get("info").asInstanceOf[BSONObject]
        val ts = x._2.get("record_time").toString.toLong
        val calories = try {
          caloriesInfo.get("calories").toString.toDouble
        } catch {
          case _ => Double.MinValue
        }
        (user, calories, ts)
      }).filter(x => x._2 != Double.MinValue && x._3 >= startTime && x._3 < endTime)
      .map(x => (x._1, x._2))
      .reduceByKey((a, b) => (a + b))
      .map(x => (x._1, x._2))

    val userMibandCalories = dataRDD.filter(x => x._2.get("device").toString.equalsIgnoreCase("mi_band"))
      .map(x => {
        val user = x._2.get("uid").toString
        val caloriesInfo = try {
          val miBandInfo = x._2.get("info").asInstanceOf[BSONObject]
          val calories = miBandInfo.get("runCalorie").toString.toDouble
          val ts = x._2.get("record_time").toString.toLong / 1000
          (calories, ts)
        } catch {
          case _ => (-1D, -1L)
        }
        (user, caloriesInfo)
      })
      .filter(x => x._2._1 > 0D && x._2._2 > 0L && x._2._2 >= startTime && x._2._2 < endTime)
      .map(x => (x._1, x._2._1))

    val userPedometerCalories = dataRDD.filter(x => x._2.get("device").toString.equalsIgnoreCase("pedometer"))
      .map(x => {
        val user = x._2.get("uid").toString
        val caloriesInfo = try {
          val pedometerInfo = x._2.get("info").asInstanceOf[BSONObject]
          val step = pedometerInfo.get("sum").asInstanceOf[BSONObject].get("step").toString.toLong
          val stepCalories = (0.0076 * 4.5 + 0.0786) * 65 * (step / 120)
          val ts = x._2.get("record_time").toString.toLong / 1000
          (stepCalories, ts)
        } catch {
          case _ => (-1D, -1L)
        }
        (user, caloriesInfo)
      }).filter(x => x._2._1 > 0D && x._2._2 > 0L && x._2._2 >= startTime && x._2._2 < endTime)
      .map(x => (x._1, x._2._1))

    val userRunCalories = dataRDD.filter(x => x._2.get("device").toString.equalsIgnoreCase("run_tracker"))
    .map(x => {
      val user = x._2.get("uid").toString
      val caloriesInfo = try {
        val runTrackerInfo = x._2.get("info").asInstanceOf[BSONObject]
        val calories = runTrackerInfo.get("calorie").toString.toDouble
        val ts = x._2.get("record_time").toString.toLong / 1000
        (calories, ts)
      } catch {
        case _ => (-1D, -1L)
      }
      (user, caloriesInfo)
    })
    .filter(x => x._2._1 > 0D && x._2._2 > 0L && x._2._2 >= startTime && x._2._2 < endTime)
    .map(x => (x._1, x._2._1))

    val userStepCalories = userPedometerCalories.fullOuterJoin(userMibandCalories)
      .map(x => {
        val user = x._1
        val calories = x._2._2 match {
          case Some(value) => value
          case _ => {
            x._2._1 match {
              case Some(value) => value
              case _ => 0D
            }
          }
        }
        (user, calories)
      })

    val userConsumedCalories = userStepCalories.fullOuterJoin(userRunCalories)
    .map(x => {
      val user = x._1
      val stepCalories = x._2._1 match {
        case Some(value) => value
        case _ => 0D
      }
      val runCalories = x._2._2 match {
        case Some(value) => value
        case _ => 0D
      }
      (user, stepCalories + runCalories)
    })

    val userCaloriesDiff = userIntakeCalories.fullOuterJoin(userConsumedCalories)
      .map(x => {
        val user = x._1
        val intakeCalories = x._2._1 match {
          case Some(value) => value
          case _ => 0D
        }
        val consumedCalories = x._2._2 match {
          case Some(value) => value
          case _ => 0D
        }
        (user, intakeCalories - consumedCalories)
      })

    userCaloriesDiff.saveAsTextFile(args(2))

  }
}
