package com.feel.issues

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject

/**
 * Created by canoe on 10/15/15.
 */
object ChatOnlyUser {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userChatNumber = mongoRDD.map(x => {
      val from = x._2.get("from").toString
      val to = x._2.get("to").toString
      (from, to)
    }).distinct()
      .map(x => (x._1, 1))
      .reduceByKey((a, b) => a + b)
      .map(x => (x._1, x._2.toString))

    val userGender = sc.textFile(args(2))
      .map(_.split(("\t")))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))

    val userAge = sc.textFile(args(3))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1).substring(0, 4)))

    def userFeatureCount(rdd: RDD[String]) = {
      rdd.map(_.split("\t"))
        .filter(_.length == 2)
        .map(x => (x(0), 1))
        .reduceByKey((a, b) => a + b)
        .map(x => (x._1, x._2.toString))
    }

    val userFollowing = userFeatureCount(sc.textFile(args(4)))
    val userLikeNumber = userFeatureCount(sc.textFile(args(5)))
    val userCommentNumber = userFeatureCount(sc.textFile(args(6)))
    val userCardNumber = userFeatureCount(sc.textFile(args(7)))

    hadoopConf.set("mongo.auth.uri", args(8))
    hadoopConf.set("mongo.input.uri", args(9))
    val deviceRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userResolution = deviceRDD.map(x => {
      val user = x._2.get("uid").toString
      val resolution = x._2.get("resolution").toString
      (user, resolution)
    }).distinct()

    val userModel = deviceRDD.map(x => {
      val user = x._2.get("uid").toString
      val model = x._2.get("phoneModel").toString
      (user, model)
    }).distinct()

    def chatUserData(aRDD: RDD[(String, String)], bRDD: RDD[(String, String)]) = {
      aRDD.join(bRDD)
        .map(x => (x._2, 1))
        .reduceByKey((a, b) => a + b)
        .map(x => x._1._1 + "\t" + x._1._2 + "\t" + x._2)
    }
    chatUserData(userChatNumber, userGender).saveAsTextFile(args(10))
    chatUserData(userChatNumber, userAge).saveAsTextFile(args(11))
    chatUserData(userChatNumber, userFollowing).saveAsTextFile(args(12))
    chatUserData(userChatNumber, userLikeNumber).saveAsTextFile(args(13))
    chatUserData(userChatNumber, userCommentNumber).saveAsTextFile(args(14))
    chatUserData(userChatNumber, userCardNumber).saveAsTextFile(args(15))
    chatUserData(userChatNumber, userResolution).saveAsTextFile(args(16))
    chatUserData(userChatNumber, userModel).saveAsTextFile(args(17))
  }
}
