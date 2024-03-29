package com.feel.statistics

import com.feel.utils.TimeIssues
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 1/18/16.
  */
object DistanceReport {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val startTime = TimeIssues.nDaysAgoTs(args(4).toInt)
    val endTime = TimeIssues.nDaysAgoTs(args(5).toInt)

    val userDistance = mongoRDD.map(x => {
      val user = x._2.get("uid").toString
      val goalType = x._2.get("goal_type").toString
      val ts = x._2.get("record_time").toString.toLong / 1000
      val distanceNumber = if (ts >= startTime && ts < endTime) {
        val distance = try {
          goalType match {
            case "3" => {
              val deviceType = x._2.get("device")
              val distance = deviceType match {
                case "mi_band" =>
                  val distanceInfo = x._2.get("info").asInstanceOf[BSONObject]
                  distanceInfo.get("walkDistance").toString.toLong + distanceInfo.get("runDistance").toString.toLong
                case _ =>
                  x._2.get("info").asInstanceOf[BSONObject].get("sum").asInstanceOf[BSONObject].get("distance")
                    .toString.toLong
              }
              distance
            }
            case _ => 0L
          }
        } catch {
          case _: Throwable => 0L
        }
        distance
      } else {
        0L
      }
      (user, (distanceNumber, ts))
    }).filter(_._2._1 != 0L)

    userDistance.map(x => (x._1, x._2._1))
      .reduceByKey((a, b) => a + b)
      .map(x => {
        val user = x._1
        val stepNumber = (x._2 / 1000.0).formatted("%.2f")
        user + "\tuser_distance:" + stepNumber.toString
      }).saveAsTextFile(args(2))

    userDistance.groupByKey()
      .map(x => {
        val user = x._1
        val maxDistanceDay = x._2.maxBy(y => y._1)
        user + "\tuser_daily_max_distance:" + (maxDistanceDay._1 / 1000.0).formatted("%.2f") + "," + maxDistanceDay._2.toString
      }).saveAsTextFile(args(3))
  }
}
