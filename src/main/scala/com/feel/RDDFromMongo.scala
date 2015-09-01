package com.feel.experiment

import org.apache.spark.SparkContext
import org.bson.BSONObject
import org.apache.hadoop.conf.Configuration

/**
 * Created by canoe on 9/1/15.
 */
object RDDFromMongo {

  def main(args: Array[String]) = {

    val sc = new SparkContext()
    val conf = new Configuration()
    conf.set("mongo.input.uri", "mongodb://xxxxx:27017/dc.hardware_data")
    val mongoRDD = sc.newAPIHadoopRDD(conf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userData = mongoRDD.filter(x => {
      x._2.get("uid").toString == "1088"
    })
    userData.saveAsTextFile(args(0))
  }

}
