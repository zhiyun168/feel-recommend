package com.feel.statistics

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 1/11/16.
  */
object NewUserLogInfo {

  private val REPORT_SET = Set(1, 3, 7, 15)

  def nDaysAgoTs(n: Int) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -n)
    dateFormat.parse(dateFormat.format(calendar.getTime)).getTime / 1000
  }

  def nDaysAgoDate(n: Int) = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -n)
    dateFormat.format(calendar.getTime)
  }

  def main(args: Array[String]) = {

    val sc = new SparkContext()

    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"))
    val startTime = nDaysAgoTs(4)
    val registeredEndTime = nDaysAgoTs(3)

    val user = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(1).toLong >= startTime && x(1).toLong < registeredEndTime)
      .map(x => (x(0), x(1)))

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(1))
    hadoopConf.set("mongo.input.uri", args(2))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val endTime = nDaysAgoTs(0)
    val userLoginInfo = mongoRDD.map(x => {
      try {
        val user = x._2.get("uid").toString
        val ts = x._2.get("login_time").toString.toLong / 1000
        if (ts >= startTime && ts < endTime) {
          val dayGap = (ts - startTime) / 86400
          (user, dayGap.toInt)
        } else {
          ("", -1)
        }
      } catch {
        case _: Throwable => ("", -1)
      }
    }).filter(_ != ("", -1))
      .distinct()
      .join(user)
      .map(x => (x._1, x._2._1))
      .groupByKey()
      .map(x => {
        val mask = x._2.foldLeft(0)((acc, value) => acc | (1 << value))
        (mask, 1)
      }).reduceByKey((a, b) => a + b)
      .collect()
      .sortWith(_._1 < _._1)
      .filter(x => REPORT_SET.contains(x._1))

    val stayedLessThanOneMinute = mongoRDD.map(x => {
      try {
        val user = x._2.get("uid").toString
        val ts = x._2.get("login_time").toString.toLong / 1000
        if (ts >= startTime && ts < endTime) {
          (user, ts.toInt)
        } else {
          ("", -1)
        }
      } catch {
        case _: Throwable => ("", -1)
      }
    }).filter(_ != ("", -1))
      .reduceByKey((a, b) => if (a > b) a else b)
      .join(user)
      .map(x => {
        val user = x._1
        val delta = x._2._1 - x._2._2.toInt
        (user, delta)
      }).filter(_._2 < 60)
        .count()

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -4)
    val date = dateFormat.format(calendar.getTime)

    def loginDateInfo(mask: Int) = {
      (0 until 4).foldLeft(List[String]()) ((acc, index) => {
        if (((1 << index) & mask) != 0) {
          acc ++ List(nDaysAgoDate(4 - index))
        } else {
          acc
        }
      }).mkString(",") + "(只在这些日期)登陆过的人数"
    }

    val total = user.count()
    val result = (date + "总注册数\t" + total.toString) ::
      ("最后登陆时间与注册时间差1分钟以内用户数(可认为只登陆过一次)\t" + stayedLessThanOneMinute.toString +
        "\t占注册用户比例\t" + (stayedLessThanOneMinute.toDouble / total * 100).formatted("%.2f").toString + "%") ::
      userLoginInfo.map(x => loginDateInfo(x._1) + "\t" + x._2.toString +
        "\t占注册用户比例\t" + (x._2.toDouble / total * 100).formatted("%.2f").toString + "%").toList

    sc.parallelize(result).saveAsTextFile(args(3))
  }
}
