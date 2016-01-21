package com.feel.statistics

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 1/21/16.
  */
object SpecificUserCount {

  def main(args: Array[String]) = {
    val sc = new SparkContext()
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val goalTypeNumber = mongoRDD.map(x => {
      val user = x._2.get("uid").toString
      val goalType = x._2.get("goal_type").toString
      (goalType, user)
      }).distinct()
    .map(x => (x._1, 1))
    .reduceByKey((a, b) => a + b)

    goalTypeNumber.saveAsTextFile(args(2))

  }
}
