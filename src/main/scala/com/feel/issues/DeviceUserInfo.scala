package com.feel.issues

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BSONObject

/**
 * Created by canoe on 10/16/15.
 */
object DeviceUserInfo {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userModel = mongoRDD.map(x => {
      val user = x._2.get("uid").toString
      val model = x._2.get("phoneModel").toString
      (user, model)
    }).distinct()

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

    def modelUserData(aRDD: RDD[(String, String)], bRDD: RDD[(String, String)]) = {
      aRDD.join(bRDD)
        .map(x => (x._2, 1))
        .reduceByKey((a, b) => a + b)
        .map(x => x._1._1 + "\t" + x._1._2 + "\t" + x._2)
    }

    modelUserData(userModel, userGender).saveAsTextFile(args(8))
    modelUserData(userModel, userAge).saveAsTextFile(args(9))
    modelUserData(userModel, userFollowing).saveAsTextFile(args(10))
    modelUserData(userModel, userLikeNumber).saveAsTextFile(args(11))
    modelUserData(userModel, userCommentNumber).saveAsTextFile(args(12))
    modelUserData(userModel, userCardNumber).saveAsTextFile(args(13))
  }
}
