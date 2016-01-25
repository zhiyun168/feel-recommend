package com.feel.issues

import com.feel.utils.TimeIssues
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 1/25/16.
  */
object RunInfo {

  def main(args: Array[String]) = {

    val sc = new SparkContext()

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val startTime = TimeIssues.nDaysAgoTs(400)
    val endTime = TimeIssues.nDaysAgoTs(0)

    val userDistanceTime = mongoRDD.filter(x => {
      val goalType = x._2.get("goal_type").toString
      val ts = x._2.get("record_time").toString.toLong / 1000
      ts >= startTime && ts < endTime && (goalType == "6")
    }).map(x => {
      val data = try {
        val user = x._2.get("uid").toString
        val goalType = x._2.get("goal_type").toString
        val ts = x._2.get("record_time").toString.toLong
        val distance = goalType match {
          case "6" =>
            x._2.get("info").asInstanceOf[BSONObject].get("distance").toString.toDouble
          case _ => 0D
        }
        (user, distance, ts)
      } catch {
        case _: Throwable => ("", 0D, 0L)
      }
      data
    }).filter(_._2 != 0D)

    userDistanceTime.map(x => (x._1, x._2)).reduceByKey((a, b) => a + b)
    .sortBy(_._2).saveAsTextFile(args(2))

    userDistanceTime.map(x => (x._1, 1)).reduceByKey((a, b) => a + b)
    .sortBy(_._2).saveAsTextFile(args(3))

    userDistanceTime.map(x => (x._1, TimeIssues.tsToDate(x._3)))
      .distinct()
      .map(x => (x._1, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(_._2).saveAsTextFile(args(4))
  }
}
