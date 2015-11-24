package com.feel.statistics

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 11/24/15.
  */

object StepNumber {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    val startTime = dateFormat.parse(args(2)).getTime() / 1000
    val endTime = dateFormat.parse(args(3)).getTime() / 1000

    val userStepNumber = mongoRDD.map(x => {
      val user = x._2.get("uid")
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
          case _ => 0
        }
        step
      } else {
        0
      }
      (user, stepNumber)
    }).filter(_._2 != 0)
    .sortBy(_._2)
    userStepNumber.saveAsTextFile(args(4))
  }
}
