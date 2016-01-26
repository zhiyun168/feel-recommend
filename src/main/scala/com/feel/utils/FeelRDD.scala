package com.feel.utils

import org.apache.spark.rdd.RDD

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