package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

/**
 * Created by canoe on 7/14/15.
 */
object recommendCardBasedOnALS {

  private val REAL_USER_ID_BOUND = 1075
  private var RANK = 0
  private var ITERATION_NUMBER = 0
  private var LAMBDA = 0D
  private val RDD_PARTITION_NUMBER = 10
  private var CARD_LIKED_NUMBER = 0
  private val CANDIDATE_SIZE = 100

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))

    val sc = new SparkContext(conf)

    val userLikedCard = sc.textFile(args(1))
      .distinct(RDD_PARTITION_NUMBER)
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1))) //user, card

    val userGender = sc.textFile(args(2))
      .distinct(RDD_PARTITION_NUMBER)
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .filter(x => x(1) == "f" || x(1) == "m")
      .map(x => (x(0), x(1))) // user, gender

    val cardInfo = sc.textFile(args(3))
      .distinct(RDD_PARTITION_NUMBER)
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1)))
      .join(userGender) //user, (card, gender)
      .map(x => (x._2._1, (x._1, x._2._2))) // card, (user, gender)


    val filteredCard = userLikedCard
      .map(x => (x._2, 1)) // card
      .reduceByKey((a, b) => a + b)
      .filter(_._2 >= CARD_LIKED_NUMBER)

    val trainData = userLikedCard
      .map(x => (x._2, x._1)) // card, user
      .join(filteredCard) //card, user, number
      .map(x => (x._2._1, x._1)) // user, card
      .join(userGender) // user, (likedCard, userGender)
      .map(x => (x._2._1, (x._1, x._2._2))) // likedCard, (user, userGender)
      .join(cardInfo)
      .filter(x => x._2._1._1 != x._2._2._1) // user like user's own card
      .map(x => {
        def getScore(userGender: String, cardGender: String) = {
          if (userGender == cardGender) 1D
          else if (userGender == "m" && cardGender == "f") 1D
          else 1D
        }

        val userGender = x._2._1._2
        val cardGender = x._2._2._2
        val user = x._2._1._1.toInt
        val item = x._1.toInt
        Rating(user, item, getScore(userGender, cardGender))
      })


    RANK = args(5).toInt
    ITERATION_NUMBER = args(6).toInt
    LAMBDA = args(7).toDouble
    CARD_LIKED_NUMBER = args(8).toInt


    val tagCard = sc.textFile(args(9)) // card, tag,
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))
      .join(filteredCard) //card, tag, number
      .map(x => (x._2._1, (x._1, x._2._2))) //tag, card, number
      .groupByKey(5 * RDD_PARTITION_NUMBER) // tag, [card, number]
      .map(x => (x._1, x._2.map(_._1)))

    val userTag = sc.textFile(args(10)) // user, tag
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))

    val userCards = userLikedCard
      .map(x => (x._1, "_")) //user
      .distinct(RDD_PARTITION_NUMBER)
      .join(userTag) //user "_" tag
      .map(x => (x._2._2, x._1)) // tag, user
      .join(tagCard) //tag, user, cardList
      .map(x => (x._2._1, x._2._2)) // user, cardList
      .groupByKey(5 * RDD_PARTITION_NUMBER)
      .flatMap(x => {
      val user = x._1
      x._2.flatten.toSeq.distinct.map(card => (user.toInt, card.toInt))
    })

    val model = ALS.train(trainData, RANK, ITERATION_NUMBER, LAMBDA)

    //userCards.saveAsTextFile(args(11))

    val predictions = model
      .predict(userCards)
      .map({case Rating(user, item, score) => (user, (item, score))})
      .filter(_._2._2 >= 0.5)
      .groupByKey(5 * RDD_PARTITION_NUMBER)
      .map(x => (x._1, x._2.toSeq.sortWith(_._2 > _._2).take(CANDIDATE_SIZE)))

    /*val likeAndPredictions = recentlyLikedRDD.map {
      case Rating(user, item, rate) =>
        ((user, item), rate)
    }.join(predictions)


    val MSE = likeAndPredictions.map { case ((user, product), (r1, r2)) =>
      (r1 - r2) * (r1 - r2)
    }.mean()

    println("Mean Squared Error = " + MSE)*/

    predictions.saveAsTextFile(args(12))
  }
}
