package com.feel.statistics

import com.feel.utils.TimeIssues
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 1/18/16.
  */
object ActivityTimeReport {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val startTime = TimeIssues.nDaysAgoTs(args(3).toInt)
    val endTime = TimeIssues.nDaysAgoTs(args(4).toInt)

    val userDistance = mongoRDD.filter(x => {
      val goalType = x._2.get("goal_type").toString
      val ts = x._2.get("record_time").toString.toLong / 1000
      ts >= startTime && ts < endTime && goalType == "13"
    }).map(x => {
      val data = try {
        val user = x._2.get("uid").toString
        val activityTime = x._2.get("sum").asInstanceOf[BSONObject].get("duration").toString.toLong
        (user, activityTime)
      } catch {
        case _: Throwable => ("", 0L)
      }
      data
    }).filter(_._2 != 0L)
      .reduceByKey((a, b) => a + b)

    userDistance.map(x => x._1 + "\tactivity_time:" + x._2.toString).saveAsTextFile(args(2))
  }
}
