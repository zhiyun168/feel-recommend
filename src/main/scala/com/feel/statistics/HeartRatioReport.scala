package com.feel.statistics

import java.text.SimpleDateFormat
import java.util.Date

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

    val startTime = TimeIssues.nDaysAgoTs(args(5).toInt)
    val endTime = TimeIssues.nDaysAgoTs(args(6).toInt)

    val userHeartRatio = mongoRDD.filter(x => {
      val ts = x._2.get("record_time").toString.toLong / 1000
      ts >= startTime && ts < endTime
    }).map(x => {
      try {
        val user = x._2.get("uid").toString
        val heartRatio = x._2.get("info").asInstanceOf[BSONObject].get("bpm").toString.toDouble
        (user, heartRatio)
      } catch {
        case _: Throwable => ("", 0D)
      }
    }).filter(_._2 > 0D)
      .groupByKey()
      .map(x => {
        val user = x._1
        val minHeartRatio = x._2.min
        val maxHeartRatio = x._2.max
        val meanHeartRatio = x._2.foldLeft(0D)((acc, value) => acc + value) / x._2.size
        (user, (minHeartRatio, maxHeartRatio, meanHeartRatio))
      })

    userHeartRatio.map(x => {
      val user = x._1
      user + "\tuser_heart_ratio: [" + x._2._1.toString + "," + x._2._2.toString + "," + x._2._3.formatted("%.2f") + "]"
    }).saveAsTextFile(args(2))


    val userInfo = sc.textFile(args(3))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .map(x => {
        val user = x(0)
        val gender = x(1)
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
        }
      })
    val infoUserHeartRatioDiff = userHeartRatio.map(x => (x._1, x._2._3))
      .join(userInfo)
      .map({ case (user, (heartRatio, info)) =>
        (info, (user, heartRatio))
      }).groupByKey()
      .flatMap(x => {
        val infoUserHeartRatioMean = x._2.foldLeft(0D)((acc, value) => acc + value._2) / x._2.size
        x._2.map({ case (user, heartRatio) =>
          user + "\tinfo_user_heart_ratio_mean: [" + x._1 + "," + infoUserHeartRatioMean.formatted("%.2f") + "]"
        })
      })
    infoUserHeartRatioDiff.saveAsTextFile(args(4))
  }
}
