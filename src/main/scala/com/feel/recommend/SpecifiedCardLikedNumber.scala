package com.feel.statistics

import org.apache.spark.SparkContext

/**
  * Created by canoe on 12/25/15.
  */
object SpecifiedCardLikedNumber {

  def main(args: Array[String]) = {
    val sc = new SparkContext()
    val cardId = sc.textFile(args(0))
      .distinct()
      .map(_.split("\t"))
      .filter(_.length == 1)
      .map(x => (x(0), ""))

    val cardLikedNumber = sc.textFile(args(1))
      .map(_.split("\t")) //user, card
      .filter(x => x.length == 2)
      .map(x => (x(1), 1))
      .reduceByKey((a, b) => a + b)

    val specifiedCardLikedNumber = cardLikedNumber.join(cardId)
      .map(x => (x._1, x._2._1))
      .sortBy(_._2)

    specifiedCardLikedNumber.saveAsTextFile(args(2))
  }
}
