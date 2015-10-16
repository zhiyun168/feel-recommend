package com.feel.issues

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by canoe on 10/14/15.
 */
object TopTagCard {

  def main(args: Array[String]) = {

    val sc = new SparkContext(new SparkConf())
    val cardTag = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2) // card, tag
      .map(x => (x(1), 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(_._2)

    val tagDataInfo = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))// tag, name
      .join(cardTag) // tag, name, number
      .map(x => (x._1 + "\t" + x._2._1 + "\t" + x._2._2))

    tagDataInfo.saveAsTextFile(args(2))
  }
}
