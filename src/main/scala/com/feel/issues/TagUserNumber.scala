package com.feel.issues

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by canoe on 12/28/15.
  */
object TopUserNumber {

  def main(args: Array[String]) = {

    val sc = new SparkContext(new SparkConf())

    val cardUser = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2) // card, user
      .map(x => (x(0), x(1)))

    val cardTag = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2) // card, tag
      .map(x => (x(0), x(1)))
      .join(cardUser) // card, (tag, user)
      .map(x => x._2)
      .groupByKey()
      .map(x => {
        (x._1, x._2.toSeq.distinct.size)
      })

    val tagDataInfo = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))// tag, name
      .join(cardTag) // tag, name, number
      .sortBy(_._2._2)
      .map(x => (x._1 + "\t" + x._2._1 + "\t" + x._2._2))

    tagDataInfo.saveAsTextFile(args(3))
  }
}
