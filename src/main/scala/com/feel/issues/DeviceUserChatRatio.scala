package com.feel.issues

import breeze.linalg.max
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BSONObject

/**
 * Created by canoe on 10/22/15.
 */
object DeviceUserChatRatio {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val hadoopConf = new Configuration()

    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userChatInfo = mongoRDD.map(x => {
      val from = x._2.get("from").toString
      (from, 1)
    }).distinct()

    hadoopConf.set("mongo.auth.uri", args(2))
    hadoopConf.set("mongo.input.uri", args(3))
    val deviceRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userDeviceInfo = deviceRDD.map(x => {
      val user = x._2.get("uid").toString
      val resolution = x._2.get("resolution").toString
      val resolutionTmp = resolution.split(",")
      val screenSize = max(resolutionTmp(0).toInt, resolutionTmp(1).toInt).toString
      (user, screenSize)
    }).distinct()

    val resolutionUserChatRatio = userDeviceInfo.leftOuterJoin(userChatInfo)
      .map(x => {
      val chatCount = x._2._2 match {
        case Some(number) => 1
        case None => 0
      }
      ((x._2._1, chatCount), 1)
    }).reduceByKey((a, b) => a + b)
      .map(x => {
      val key = x._1._1
      val value = (x._1._2, x._2)
      (key, value)
    }).groupByKey()
      .map(x => {
      val deviceAllUser = x._2.map(_._2).sum
      val chatNumber = {
        val chatList = x._2.filter(_._1 == 1)
        if (chatList.size == 0) 0 else chatList.map(_._2).head
      }
      if (deviceAllUser >= 100)
        (x._1, x._2, chatNumber.toDouble / deviceAllUser)
      else
        (x._1, x._2, -1.0)
    }).filter(_._3 > 0)
      .map(x => x._1 + "\t" + x._3)
    resolutionUserChatRatio.saveAsTextFile(args(4))

    val resolutionModel = deviceRDD.map(x => {
      val resolution = x._2.get("resolution").toString
      val resolutionTmp = resolution.split(",")
      val screenSize = max(resolutionTmp(0).toInt, resolutionTmp(1).toInt).toString
      val model = x._2.get("phoneModel").toString.replaceAll("[\t ]", "")
      (screenSize, model)
    }).distinct()
      .groupByKey()
      .map(x => {
      val screenSize = x._1
      val modelList = x._2.toSeq.distinct.mkString(",")
      screenSize + "\t" + modelList
    })
    resolutionModel.saveAsTextFile(args(5))

    val userModelInfo = deviceRDD.map(x => {
      val user = x._2.get("uid").toString
      val model = x._2.get("phoneModel").toString.replaceAll("[\t ]", "")
      (user, model)
    }).distinct()

    val modelUserChatRatio = userModelInfo.leftOuterJoin(userChatInfo)
      .map(x => {
      val chatCount = x._2._2 match {
        case Some(number) => 1
        case None => 0
      }
      ((x._2._1, chatCount), 1)
    }).reduceByKey((a, b) => a + b)
      .map(x => {
      val key = x._1._1
      val value = (x._1._2, x._2)
      (key, value)
    }).groupByKey()
      .map(x => {
      val deviceAllUser = x._2.map(_._2).sum
      val chatNumber = {
        val chatList = x._2.filter(_._1 == 1)
        if (chatList.size == 0) 0 else chatList.map(_._2).head
      }
      if (deviceAllUser >= 100)
        (x._1, x._2, chatNumber.toDouble / deviceAllUser)
      else
        (x._1, x._2, -1.0)
    }).filter(_._3 > 0)
    .map(x => (x._1, x._3))

    val modelUserNumber = deviceRDD.map(x => {
      val user = x._2.get("uid").toString
      val model = x._2.get("phoneModel").toString.replaceAll("[\t ]", "")
      (model, user)
    }).distinct()
    .groupByKey().map(x => (x._1, x._2.size))
    .filter(_._2 > 100)

    val modelUserGreaterThan100ChatRatio = modelUserChatRatio.join(modelUserNumber)
    .map(x => x._1 + "\t" + x._2._1 + "\t" + x._2._2)

    modelUserGreaterThan100ChatRatio.saveAsTextFile(args(6))
  }
}
