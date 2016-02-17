package com.feel.statistics

import com.feel.utils.TimeIssues
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 1/15/16.
  */
object CaloryReport {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val startTime = TimeIssues.nDaysAgoTs(args(3).toInt)
    val endTime = TimeIssues.nDaysAgoTs(args(4).toInt)

    val userRunCalories = mongoRDD
      .filter(x => {
        val goalType = x._2.get("goal_type").toString
        val ts = x._2.get("record_time").toString.toLong / 1000
        (goalType == "6" && ts >= startTime && ts < endTime)
      }).map(x => {
      val user = x._2.get("uid").toString
      val runCalories = x._2.get("info").asInstanceOf[BSONObject].get("calorie").toString.toDouble
      (user, runCalories)
    })

    val userStepCalories = mongoRDD.map(x => {
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
    .map({ case (user, step) => {
      val stepCalories = (0.0076 * 4.5 + 0.0786) * 65 * (step / 120)
      (user, stepCalories)
    }})


    userRunCalories.union(userStepCalories).reduceByKey((a, b) => a + b)
      .map(x => {
        x._1 + "\tuser_calories:" + x._2.formatted("%.2f")
      })
      .saveAsTextFile(args(2))

  }
}
