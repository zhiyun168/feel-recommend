package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable
import scala.util.Random._
import org.elasticsearch.spark._

/**
 * Created by canoe on 9/15/15.
 */

case class GenderCandidates(gender: String, candidates: Seq[String])

object RecommendHistoryHotContextBasedOnGender {

  private val REAL_USER_ID_BOUND = 1075
  private val CANDIDATES_SIZE = 200

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    conf.set("es.mapping.id", "gender")
    conf.set("es.nodes", args(0))
    val sc = new SparkContext(conf)

    val userGender = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1)))

    val cardOwnerGender = sc.textFile(args(2))
    .map(_.split("\t")) // user, card, type
    .filter(x => x.length == 3 && x(0).toInt >= REAL_USER_ID_BOUND && x(2) == "card")
    .map(x => (x(0), x(1))) // user card
    .join(userGender) // user, (card, gender)
    .map(x => (x._2._1, (x._1, x._2._2))) // (card, (user, gender))

    val cardLikedNumber = sc.textFile(args(3))
    .map(_.split("\t"))// user, card id
    .filter(x => x.length == 2 && x(0).toInt >= REAL_USER_ID_BOUND)
    .map(x => (x(1), 1))
    .reduceByKey((a, b) => a + b) // card, number

    val result = cardLikedNumber.join(cardOwnerGender) // card, (likedNumber, (user, gender))
    .map(x => {
      (if (x._2._2._2 == "f") "1" else "-1", (x._2._2._1, (x._2._1, x._1))) //gender, (user, likedNumber, card)
    }).groupByKey() // biao ge shuo 1 male -1 female
    .map(x => {
      val key = x._1
      if (key == "1") {
        val candidates = x._2.foldLeft(new mutable.HashMap[String, (Int, String)]())((userCardCmp, tuple) => {
          val previousValue = userCardCmp.getOrElse(tuple._1, (0, ""))
          if (previousValue._1 < tuple._2._1)
            userCardCmp(tuple._1) = tuple._2
          userCardCmp
        }).toList.map(_._2).sortWith(_._1 > _._1).map(_._2).take(CANDIDATES_SIZE)
        GenderCandidates(key, candidates)
      } else {
        val candidates = x._2.map(_._2).toSeq.sortWith(_._1 > _._1).map(_._2).take(CANDIDATES_SIZE)
        GenderCandidates(key, candidates)
      }
    })
    result.saveAsTextFile(args(4))
    result.saveToEs("recommendation/newUserGenderContext")


    /*val cardCandidates = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1))) // user, card
      .join(userGender) // user, (card, gender)
      .map(x => (x._2, 1)) // (card, gender), 1
      .reduceByKey((a, b) => a + b)
      .map(x => (x._1._1, (x._1._2, x._2))) // card, (gender, number)
      .groupByKey()
      .map(x => {
      def cardLikedGenderInfo(x: Iterable[(String, Int)]) = {
        val genderMap = x.map(x => if (x._1 == "f") (x._1, x._2 * 3) else x).toMap
        val genderOption = List("f", "m", "x")
        genderOption.foldLeft((0, "x"))((maxGenderInfo, gender) => {
          val genderNumber = genderMap.getOrElse(gender, 0)
          if (genderNumber > maxGenderInfo._1)
            (genderNumber, gender)
          else
            maxGenderInfo
        })
      }
      (cardLikedGenderInfo(x._2)._2, x._1) // gender, card
    }).groupByKey()
    .map(x => {
      val gender = x._1

      def knuthShuffle[T](x: Array[T]) = {
        for (i <- (1 until x.size).reverse) {
          val j = nextInt(i + 1)
          val tmp = x(i)
          x(i) = x(j)
          x(j) = tmp
        }
        x
      }

      val hotCardCandidates = knuthShuffle(x._2.toArray).take(CANDIDATES_SIZE).mkString(",")
      (gender, hotCardCandidates)
    })

    cardCandidates.saveAsTextFile(args(2))
    */
  }
}
