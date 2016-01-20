package com.feel.statistics

import com.feel.utils.TimeIssues
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 1/15/16.
  */
object WeeklyDayStepTrend {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val startTime = TimeIssues.nDaysAgoTs(7)
    val endTime = TimeIssues.nDaysAgoTs(0)

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
      (user, (stepNumber, ts))
    }).filter(_._2._1 != 0)
      .groupByKey()
      .map({case (user, dailyStepList) => {
        val stepList = dailyStepList.toList.sortWith(_._1 < _._1)
        user + "\tstep_trend:" + stepList.sortWith(_._2 < _._2)W  .mkString(",")
      }})

    userStepNumber.saveAsTextFile(args(2))
  }
}
