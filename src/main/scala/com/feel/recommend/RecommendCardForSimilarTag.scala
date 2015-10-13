package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark._



/**
 * Created by canoe on 0/9/15.
 */
object RecommendCardForSimilarTag {

  private val LIKED_NUMBER_LOWER_BOUND = 5
  private val LIKED_NUMBER_UPPER_BOUND = 50
  private val CANDIDATES_SIZE = 100

  case class TagCandidates(tag: String, candidates: Seq[String])

  def main(args: Array[String]) = {

    val conf = new SparkConf()

    conf.set("es.mapping.id", "tag")
    conf.set("es.nodes", args(0))

    val sc = new SparkContext(conf)

    val cardTag = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1))) // card, tag

    val card = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(x => x.length == 2 && x(1) == "card")
      .map(x => (x(0), 1)) // card info

    val cardLikedNumber = sc.textFile(args(3))
      .map(_.split("\t")) // user, card
      .filter(_.length == 2)
      .map(x => (x(1), 1)) //card, number
      .join(card)
      .map(x => (x._1, x._2._1))
      .reduceByKey((a, b) => a + b) // card, number
      .filter(x => x._2 >= LIKED_NUMBER_LOWER_BOUND && x._2 <= LIKED_NUMBER_UPPER_BOUND)

    val similarTag = sc.textFile(args(4)) // tag, similarTag
      .map(_.split("\t"))
      .filter(_.length == 2)
      .flatMap(x => x(1).split(",").map(y => (y, x(0))).toSeq) // similarTag, rawTag

    val result = cardTag.join(cardLikedNumber)
      .map(x => (x._2._1, (x._1, x._2._2))) //tag, card, cardNumber
      .join(similarTag) // similarTag, rawTag
      .map(x => (x._2._2, x._2._1))// rawTag, (card, cardNumber)
      .groupByKey()
      .map(x => {
      val tag = x._1
      val candidates = x._2.toArray.sortWith(_._2 > _._2).map(_._1).distinct.take(CANDIDATES_SIZE)
      (tag, candidates)
    })

    result.map(x => (x._1, x._2.mkString(","))).saveAsTextFile(args(5))
    result.map(x => TagCandidates(x._1, x._2)).saveToEs("recommendation/similar_tag_card")
  }

}
