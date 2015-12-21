package com.feel.statistics

import java.text.SimpleDateFormat
import java.util.{TimeZone, Calendar, Date}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 12/21/15.
  */
object SameAgeUserHealthInfo {

  def getLastPastWeekBeginEndDay() = {
    val calender = Calendar.getInstance(TimeZone.getDefault())
    val date = calender.getTime()
    val dayGap = calender.get(Calendar.DAY_OF_WEEK) - Calendar.SUNDAY
    date.setTime(date.getTime() - (dayGap * 1000 * 60 * 60 * 24))
    calender.setTime(date)
    val sundayTs = calender.getTimeInMillis / 1000
    date.setTime(date.getTime() - ((dayGap + 5) * 1000 * 60 * 60 * 24))
    calender.setTime(date)
    val mondayTs = calender.getTimeInMillis / 1000
    (mondayTs, sundayTs)
  }

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

    val tsTuple = getLastPastWeekBeginEndDay()
    val (startTime, endTime) = tsTuple
    val dataRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userSleepInfo = dataRDD.filter(x => x._2.get("device").toString.equalsIgnoreCase("mi_band"))
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
      }).saveAsTextFile(args(4))

    val userStepNumber = dataRDD.map(x => {
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
          case _ => 0
        }
        step
      } else {
        0
      }
      (user, stepNumber)
    }).filter(_._2 != 0)
      .groupByKey()
      .map(x => {
        val user = x._1
        val mean = x._2.foldLeft((0D))((acc, value) => {
          acc + value
        }) / x._2.size
        (user, mean)
      })

    userInfo.join(userStepNumber)
      .map(x => {
        x._2
      }).groupByKey()
      .map(x => {
        val key = x._1._1
        val stepInfo = x._2.foldLeft(0D)((acc, value) => {
          acc + value
        }) / x._2.size
        (key, stepInfo)
      }).saveAsTextFile(args(5))

    val userBodyInfo = dataRDD.filter(x => x._2.get("device").toString.equalsIgnoreCase("picooc"))
      .map(x => {
        val user = x._2.get("uid").toString
        val bodyInfo = x._2.get("info").asInstanceOf[BSONObject]
        val fatLevel = try {
          bodyInfo.get("viseral_fat_level").toString.toDouble
        } catch {
          case _ => Double.MinValue
        }
        (user, fatLevel)
      }).filter(_._2 == Double.MinValue)
      .groupByKey()
      .map(x => (x._1,
        x._2.foldLeft(0D)((acc, value) => {
          acc + value
        }) / x._2.size)
      )

    userInfo.join(userBodyInfo)
      .map(x => {
        x._2
      }).groupByKey()
      .map(x => {
        val key = x._1._1
        val stepInfo = x._2.foldLeft(0D)((acc, value) => {
          acc + value
        }) / x._2.size
        (key, stepInfo)
      }).saveAsTextFile(args(6))

  }
}
