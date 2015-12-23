package com.feel.statistics

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 12/23/15.
  */
object ChannelIOSDataInfo {

  def getYesterdayBeginEndTs() = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    val endTime = dateFormat.parse(dateFormat.format(calendar.getTime)).getTime / 1000
    calendar.add(Calendar.DATE, -1)
    val startTime = dateFormat.parse(dateFormat.format(calendar.getTime)).getTime / 1000
    (startTime, endTime)
  }

  def main(args: Array[String]) = {
    val sc = new SparkContext()
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val (startTime, endTime) = getYesterdayBeginEndTs()
    val data = mongoRDD.filter(x => x._2.get("client").toString.equalsIgnoreCase("ios"))
      .map(x => {
        try {
          val user = x._2.get("uid").toString
          val idfa = x._2.get("muid").toString
          val registerTime = x._2.get("registered").toString.toLong
          (user, idfa, registerTime)
        } catch {
          case _ => ("", "", -1L)
        }
      }).filter(x => x._3 != -1L && x._3 >= startTime && x._3 < endTime)
      .sortBy(_._1)
      .map(x => {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        x._1 + "\t" + x._2 + "\t" + dateFormat.format(x._3 * 1000L)
      })
    data.saveAsTextFile(args(2))
  }
}
