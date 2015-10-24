package com.feel.issues

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BSONObject

/**
 * Created by canoe on 10/24/15.
 */
object SpecifiedDeviceUserInfo {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext()
    val hadoopConf = new Configuration()

    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val deviceRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val modelUser = deviceRDD.map(x => {
      val user = x._2.get("uid").toString
      val model = x._2.get("phoneModel").toString.replaceAll("[\t ]", "")
      (model, user)
    }).distinct()

    val modelList = sc.parallelize(List("Che2-TL00M", "SM705", "SM-A8000", "SM-N900", "SM-G9200", "GT-I9508",
      "2014812", "HMNOTE1LTETD", "iPad5,3", "M351", "GT-I9300", "SM-A5000", "H60-L02", "HUAWEIP7-L07", "vivoX3t"))
    .map(x => (x, "_"))

    val specifiedModelUser = modelList.join(modelUser)
    .map(x => (x._2._2, x._1)) // user, model

    def userFeatureCount(rdd: RDD[String]) = {
      rdd.map(_.split("\t"))
        .filter(_.length == 2)
        .map(x => (x(0), 1))
        .reduceByKey((a, b) => a + b)
        .map(x => (x._1, x._2))
    }

    val userFollowing = userFeatureCount(sc.textFile(args(2)))
    val userLikeNumber = userFeatureCount(sc.textFile(args(3)))
    val userCommentNumber = userFeatureCount(sc.textFile(args(4)))
    val userCardNumber = userFeatureCount(sc.textFile(args(5)))

    val allMean = sc.parallelize(List(
      ("following", userFollowing.map(_._2).mean()),
      ("like", userLikeNumber.map(_._2).mean()),
      ("comment", userCommentNumber.map(_._2).mean()),
      ("cardNumber", userCardNumber.map(_._2).mean())))
      .map(x => "All\t" + x._1 + "\t" + x._2)
    allMean.saveAsTextFile(args(6))

    def specifiedModelUserMean(aRDD: RDD[(String, String)], bRDD: RDD[(String, Int)], featureInfo: String) = {
      aRDD.join(bRDD) //user, (model, feature)
        .map(x => x._2)
        .groupByKey()
        .map(x => {
        val model = x._1
        val mean = (x._2.toSeq.sum + 0.0) / x._2.toSeq.length
        model + "\t" + featureInfo + "\t" + mean
      })
    }
    specifiedModelUserMean(specifiedModelUser, userFollowing, "following").saveAsTextFile(args(7))
    specifiedModelUserMean(specifiedModelUser, userLikeNumber, "like").saveAsTextFile(args(8))
    specifiedModelUserMean(specifiedModelUser, userCommentNumber, "comment").saveAsTextFile(args(9))
    specifiedModelUserMean(specifiedModelUser, userCardNumber, "cardNumber").saveAsTextFile(args(10))
  }
}
