package com.feel.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by canoe on 10/8/15.
 */
object FeelValueStatistics {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(args(0))
      .map(_.replaceAll("[()]", "").split(","))

    def groupData(index: Int, rdd: RDD[Array[String]]) = {
      rdd.map(x => (x(index).toInt, 1))
         .reduceByKey((a, b) => a + b)
    }

    for (i <- 1 until 6) {
      groupData(i, dataRDD).saveAsTextFile(args(i))
    }
  }
}
