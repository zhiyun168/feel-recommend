package com.feel.recommend

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject
import org.elasticsearch.spark._

case class SpeedAndHeartRatio(user: String, heartRatio: String)

/**
  * Created by canoe on 12/16/15.
  */
object RecommendSpeedAndHeartRatio {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val userMaxHeartRatio = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => {
        val user = x(0)
        val birthday = x(1)
        val pattern = "([0-9]+)-([0-9][0-9])-([0-9][0-9])".r
        val age = birthday match {
          case pattern(birthdayYear, birthdayMonth, birthdayDay) => {
            val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            val yearDiff = today.substring(0, 4).toInt - birthdayYear.toInt
            val delta = {
              if (today.substring(5, 10) > birthdayMonth + "-" + birthdayDay) 1
              else 0
            }
            yearDiff + delta
          }
          case _: String => 22
        }
        (user, (220 - age).toString)
      })

    val hadoopConf = new Configuration()

    hadoopConf.set("mongo.auth.uri", args(1))
    hadoopConf.set("mongo.input.uri", args(2))

    val sleepRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userSleepInfo = sleepRDD.filter(x => x._2.get("device").toString.equalsIgnoreCase("mi_band"))
      .map(x => {
        val user = x._2.get("uid").toString
        val sleepInfo = try {
          val miBandInfo = x._2.get("info").asInstanceOf[BSONObject]
          val shallowSleepTime = miBandInfo.get("shallowSleepTime").toString.toDouble
          val deepSleepTime = miBandInfo.get("deepSleepTime").toString.toDouble
          val ts = miBandInfo.get("created").toString.toLong
          (shallowSleepTime, deepSleepTime, ts)
        } catch {
          case _ => (-1D, -1D, -1L)
        }
        (user, sleepInfo)
      }).filter(_._1 != -1D)
      .reduceByKey((a, b) => if (a._3 < b._3) b else a)
      .map(x => {
        (x._1, if (x._2._1 + x._2._2 > 360 && x._2._2 > 180) "goodSleep" else "badSleep")
      })

    val fixedUserMaxHeartRatio = userMaxHeartRatio.union(userSleepInfo)
      .groupByKey()
      .map(x => {
        val user = x._1
        val fixedMaxHeartRatio = x._2.foldLeft(0)((acc, value) => {
            val delta = value match {
              case "badSleep" => -20
              case "goodSleep" => 0
              case _ => value.toInt
            }
            acc + delta
          })
        (user, fixedMaxHeartRatio)
      })
    fixedUserMaxHeartRatio.map(x => SpeedAndHeartRatio(x._1, x._2.toString)).saveToEs("user/fixedmaxheartratio")
    fixedUserMaxHeartRatio.saveAsTextFile(args(3))
  }
}
