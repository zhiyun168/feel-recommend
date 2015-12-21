package com.feel.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 12/21/15.
  */
object SameAgeUserHealthInfo {

  def main(args: Array[String]) = {

    val sc = new SparkContext()

    val userGender = sc.textFile(args(0))
      .map(x => x.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))

    val userAge = sc.textFile(args(1))
      .map(x => x.split("\t"))
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
        (user, age)
      })
      .filter(x => x._2 >= 10 && x._2 <= 130)


    val userInfo = userGender.leftOuterJoin(userAge)
      .map(x => {
        (x._1, (x._2._1,
        x._2._2 match {
          case Some(age) => age
          case None => -1
        }))
      })

    val hadoopConf = new Configuration()

    hadoopConf.set("mongo.auth.uri", args(2))
    hadoopConf.set("mongo.input.uri", args(3))

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    val startTime = dateFormat.parse(args(4)).getTime() / 1000
    val endTime = dateFormat.parse(args(5)).getTime() / 1000
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
          case _ => (-1D, -1D, -1D)
        }
        (user, sleepInfo)
      }).filter(x => x._2._1 != -1D && x._2._3 >= startTime && x._2._3 < endTime)
      .groupByKey()
      .map(x => {
        val user = x._1
        val sleepTimeSum = x._2.foldLeft((0D, 0D))((acc, value) => {
          (acc._1 + value._1, acc._2 + value._2)
        })
        val sleepMean = (sleepTimeSum._1 / x._2.size, sleepTimeSum._2 / x._2.size)
        (user, sleepMean)
      })

    userInfo.join(userSleepInfo)
      .map(x => {
        x._2
      }).groupByKey()
      .map(x => {
        val key = x._1._1 + "\t" + x._1._2
        val sleepInfo = x._2.foldLeft((0D, 0D))((acc, value) => {
          (acc._1 + value._1, acc._2 + value._2)
        })

        val sleepMean = (sleepInfo._1 / x._2.size, sleepInfo._1 / x._2.size)
        (key, sleepMean)
      }).saveAsTextFile(args(6))
  }
}
