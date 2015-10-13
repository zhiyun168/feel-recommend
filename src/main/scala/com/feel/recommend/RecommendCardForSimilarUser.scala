package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark._

/**
 * Created by canoe on 10/9/15.
 */
object RecommendCardForSimilarUser {


  private val LIKED_NUMBER_LOWER_BOUND = 5
  private val LIKED_NUMBER_UPPER_BOUND = 50
  private val CANDIDATES_SIZE = 100

  case class TagCandidates(user: String, candidates: Seq[String])

  def main(args: Array[String]) = {

    val conf = new SparkConf()

    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))

    val sc = new SparkContext(conf)

    val card = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(x => x.length == 2 && x(1) == "card")
      .map(x => (x(0), 1)) // card info

    val cardLikedNumber = sc.textFile(args(2))
      .map(_.split("\t")) // user, card
      .filter(_.length == 2)
      .map(x => (x(1), 1)) //card, number
      .join(card)
      .map(x => (x._1, x._2._1))
      .reduceByKey((a, b) => a + b) // card, number
      .filter(x => x._2 >= LIKED_NUMBER_LOWER_BOUND && x._2 <= LIKED_NUMBER_UPPER_BOUND)

    val cardOwner = sc.textFile(args(3)) //user, card
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(1), x(0))) // card, owner

    val cardInfo = cardOwner.join(cardLikedNumber) // card, (owner, likedNumber)
      .map(x => (x._2._1, (x._1, x._2._2))) //owner, (card, likedNumber)

    val similarUser = sc.textFile(args(4)) // user, similarUser
      .map(_.split("\t"))
      .filter(_.length == 2)
      .flatMap(x => x(1).split(",").map(y => (y, x(0))).toSeq) // similarUser, rawUser

    val result = similarUser.join(cardInfo) // similarUser, rawUser
      .map(x => (x._2._1, x._2._2)) // (rawUser, cardInfo)
      .groupByKey()
      .map(x => {
      val user = x._1
      val candidates = x._2.toArray.sortWith(_._2 > _._2).map(_._1).distinct.take(CANDIDATES_SIZE)
      (user, candidates)
    })

    result.map(x => (x._1, x._2.mkString(","))).saveAsTextFile(args(5))
    result.map(x => TagCandidates(x._1, x._2)).saveToEs("recommendation/similar_user_card")
  }
}