package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by canoe on 9/14/15.
 */

case class TagCardRecommend(user: String, candidates: Seq[String])

object RecommendTagGoodContext {

  private var CANDIDATE_SIZE = 50
  private val HEAD_SIZE = 10

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val hotTag = sc.textFile(args(0))
      .flatMap(_.split(","))
      .map(x => (x, ""))

    val cardTag = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(1), x(0))) // tag, card
      .join(hotTag)
      .filter(_._2._2 == "")
      .map(x => (x._2._1, x._1)) // card, tag

    val card = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(x => x.length == 2 && x(1) == "card")
      .map(x => (x(0), 1)) // card info

    val cardLikedNumber = sc.textFile(args(3))
      .map(_.split("\t")) // user, card
      .filter(_.length == 2)
      .map(x => (x(1), 1))
      .join(card)
      .map(x => (x._1, x._2._1))
      .reduceByKey((a, b) => a + b) // card, number

    CANDIDATE_SIZE = args(4).toInt

    val tagHistoryGoodContext = cardTag.join(cardLikedNumber)
      .map({case(card, (tag, number)) => (tag, (number, card))})
      .groupByKey()
      .map(x => x match {
      case(tag, cardList) => {
        val cardCandidates = {
          if (tag == "2066484")
            cardList.toSeq.sortWith(_._1 > _._1).map(_._2).take(CANDIDATE_SIZE)
          else
            cardList.toSeq.sortWith(_._1 > _._1).map(_._2).drop(HEAD_SIZE).take(CANDIDATE_SIZE)
        }
        TagCardRecommend(tag, cardCandidates)
      }
    })
    tagHistoryGoodContext.saveAsTextFile(args(5))
  }
}
