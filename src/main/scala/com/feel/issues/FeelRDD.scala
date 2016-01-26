package com.feel.utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.BSONObject

/**
  * Created by canoe on 1/23/16.
  */
abstract class FeelRDD(rdd: RDD[String], schema: List[String]) extends Serializable{
  private val schema_ = schema
  private val rdd_ = rdd

  def transform(): RDD[Array[String]]
  def getRDD = this.rdd_
  def getSchema() = this.schema_
}

// schema1, schema2
case class FeelDataRDD(rdd: RDD[String], schema: List[String])
  extends FeelRDD(rdd: RDD[String], schema: List[String]) {

  override def transform(): RDD[Array[String]] = super.getRDD.map(_.split("\t"))
    .filter(_.length == super.getSchema().size)

}

// user, schema1, schema2
case class FeelUserRDD(rdd: RDD[String], schema: List[String], userIndex: Int)
  extends FeelRDD(rdd: RDD[String], schema: List[String]) {

  private val REAL_USER_ID_BOUND_ = 1075

  override def transform(): RDD[Array[String]] = super.getRDD.map(_.split("\t"))
    .filter(_.length == super.getSchema().size)
    .filter(_(userIndex).toInt > REAL_USER_ID_BOUND_)
}

class FeelUserAggregatedRDD(rdd: RDD[String], schema: List[String], userIndex: Int, featureIndex: Int,
                            aggregateFunc: String => Int, ts: Long, tsIndex: Int)
  extends FeelUserRDD(rdd: RDD[String], schema: List[String], userIndex: Int) {

  def countUserInfo() = {
    transform()
      .filter(x => x(tsIndex).toLong > ts)
      .map(x => (x(0), aggregateFunc(x(featureIndex))))
      .reduceByKey((a, b) => a + b)
      .sortBy(_._2)
  }

}

class FeelUserHardWareRDD(rdd: RDD[(Object, BSONObject)], goalType: String,
                           timeRange: (Long, Long), dataSchema: String) {

  private val (startTime_, endTime_) = timeRange
  private val goalType_ = goalType
  private val rdd_ = rdd
  private val dataSchema_ = dataSchema

  def transform() {
    rdd_.filter(x => {
      val ts = x._2.get("record_time").toString.toLong / 1000
      val goalType = x._2.get("goal_type").toString
      goalType == goalType_ && ts >= startTime_ && ts < endTime_
    }).map(x => {
      try {
        val user = x._2.get("uid").toString
        val value = x._2.get("info").asInstanceOf[BSONObject].get(dataSchema_).toString.toDouble
        val ts = x._2.get("record_time").toString.toLong / 1000
        (user, value, ts)
      } catch {
        case _: Throwable => ("", 0D, 0L)
      }
    }).filter(_._2 != 0D)
  }
}

class MongoReader(authUri: String, inputUri: String) {

  def initRDD() {
    val sc = new SparkContext()

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", authUri)
    hadoopConf.set("mongo.input.uri", inputUri)
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])
  }
}