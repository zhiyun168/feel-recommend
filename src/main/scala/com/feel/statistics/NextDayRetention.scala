package com.feel.statistics

import java.util.TimeZone

import com.feel.utils.TimeIssues
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 1/14/16.
  */
object NextDayRetention {

  private val ANDROID_REGEX = "[a-zA-Z0-9_]+android[_a-zA-z0-9]*"
  private val IOS_REGEX = "[a-zA-Z0-9_]+ios"


  def main(args: Array[String]) = {

    val sc = new SparkContext()

    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"))
    val nDaysAgo = args(4).toInt
    val startTime = TimeIssues.nDaysAgoTs(nDaysAgo)
    val registeredEndTime = TimeIssues.nDaysAgoTs(nDaysAgo - 1)

    val user = sc.textFile(args(0))
      .map(_.replaceAll(ANDROID_REGEX, "android"))
      .map(_.replaceAll(IOS_REGEX, "ios"))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(2).toLong >= startTime && x(2).toLong < registeredEndTime)
      .flatMap(x => {
        ((x(0), x(1)), x(2)) :: ((x(0), "all"), x(2)) :: Nil
      }).distinct()

    val userNumber = user.map(x => (x._1._2, 1))
      .reduceByKey((a, b) => a + b)

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(1))
    hadoopConf.set("mongo.input.uri", args(2))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val loginStartTime = TimeIssues.nDaysAgoTs(nDaysAgo - 1)
    val loginEndTime = TimeIssues.nDaysAgoTs(nDaysAgo - 2)
    val userLoginInfo = mongoRDD.flatMap(x => {
      try {
        val user = x._2.get("uid").toString
        val ts = x._2.get("login_time").toString.toLong / 1000
        val client = x._2.get("client").toString.toLowerCase
        if (ts >= loginStartTime && ts < loginEndTime) {
          ((user, client), "_") :: ((user, "all"), "_") :: Nil
        } else {
          (("", ""), "") :: Nil
        }
      } catch {
        case _: Throwable => (("", ""), "") :: Nil
      }
    }).filter(_ != (("", ""), ""))
      .distinct()
      .join(user)

    sc.parallelize(List(loginStartTime, loginEndTime)).saveAsTextFile(args(5))

    val userLoginNumber = userLoginInfo
      .map(x => (x._1._2, 1))
      .reduceByKey((a, b) => a + b)

    val dateInfo = TimeIssues.nDaysAgoDate(nDaysAgo)
    val header = "平台\t次日登陆数\t注册数\t留存"
    val nextDayRetention = userNumber.join(userLoginNumber).map(x => {
       x._1 + "\t" + x._2._2.toString + "\t" + x._2._1.toString + "\t" +
         (100.0 * x._2._2 / x._2._1).formatted("%.2f") + "%"
    }).collect().toList.sortWith(_ < _)

   sc.parallelize((dateInfo + "注册用户:") :: header :: nextDayRetention).saveAsTextFile(args(3))

  }
}
