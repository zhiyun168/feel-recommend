package com.feel.statistics

import com.feel.utils.TimeIssues
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 1/21/16.
  */
object HeartRatioReport {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val startTime = TimeIssues.nDaysAgoTs(7)
    val endTime = TimeIssues.nDaysAgoTs(0)

    val userDistance = mongoRDD.filter(x => {
      val ts = x._2.get("record_time").toString.toLong / 1000
      ts >= startTime && ts < endTime
    }).map(x => {
      try {
        val user = x._2.get("uid").toString
        val heartRatio = x._2.get("info").asInstanceOf[BSONObject].get("bpm").toString
        (user, heartRatio)
      } catch {
        case _: Throwable => ("", "")
      }
    }).filter(_._2 != "")
      .groupByKey()
      .map(x => {
        val user = x._1
        val variance = x._2.toSeq.mkString(",")
        (user, variance)
      })
    userDistance.saveAsTextFile(args(2))

  }
}
