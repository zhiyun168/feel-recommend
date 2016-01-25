package com.feel.issues

import com.feel.utils.TimeIssues
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 1/18/16.
  */
object VideoTime {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val startTime = TimeIssues.nDaysAgoTs(400)
    val endTime = TimeIssues.nDaysAgoTs(0)

    val userVideoTime = mongoRDD.filter(x => {
      val goalType = x._2.get("goal_type").toString
      val ts = x._2.get("record_time").toString.toLong / 1000
      ts >= startTime && ts < endTime && (goalType == "2")
    }).map(x => {
      val data = try {
        val user = x._2.get("uid").toString
        val goalType = x._2.get("goal_type").toString

        val activityTime = goalType match {
          case "2" =>
            x._2.get("info").asInstanceOf[BSONObject].get("total_duration").toString.toDouble
          case _ => 0D
        }
        (user, activityTime)
      } catch {
        case _: Throwable => ("", 0D)
      }
      data
    }).filter(_._2 != 0D)
      .reduceByKey((a, b) => a + b)
      .sortBy(_._2)

    userVideoTime.saveAsTextFile(args(2))
  }
}
