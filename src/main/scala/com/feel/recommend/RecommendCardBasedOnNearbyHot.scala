package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random._
import org.elasticsearch.spark._

/**
  * Created by canoe on 11/20/15.
  */
object RecommendCardBasedOnNearbyHot {


  private val SAMPLE_THRESHOLD = 1500
  private val REAL_USER_ID_THRESHOLD = 1075
  private val CANDIDATES_SIZE = 100
  private val DISTANCE_THRESHOLD = 10000D

  case class NearByCandidates(user: String, candidates: Seq[String])

  def knuthShuffle[T](x: Array[T]) = {
    for (i <- (1 until x.length).reverse) {
      val j = nextInt(i + 1)
      val tmp = x(i)
      x(i) = x(j)
      x(j) = tmp
    }
    x
  }


  def distance(gpsTupleA: (Double, Double), gpsTupleB: (Double, Double)) = {

    def degree2radius(degree: Double) = degree * Math.PI / 180.0

    def radius2degree(radius: Double) = radius / Math.PI * 180.0

    val R = 60 * 1.1515 * 1.609344 * 1000

    val (latA, lonA) = gpsTupleA
    val (latB, lonB) = gpsTupleB

    val theta = lonA - lonB
    val distance = Math.sin(degree2radius(latA)) * Math.sin(degree2radius(latB)) +
      Math.cos(degree2radius(latA)) * Math.cos(degree2radius(latB)) * Math.cos(degree2radius(theta))

    Math.abs(Math.round(radius2degree(Math.acos(distance)) * R))
  }

  def main(args: Array[String]) {

    val conf = new SparkConf()

    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))

    val sc = new SparkContext(conf)

    val recentlyActiveUser = sc.textFile(args(4))
      .map(x => x.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))

    val userNearby = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 4)
      .map(x => {
        try {
          //f	[lat#30.051169,lon#119.930501]	1438507097563	[_index#location_v1,_type#user_loc,_id#1203935,_score#0.0]
          val locationInfo = x(1).replaceAll("[\\[\\]]", "").split(",").sortWith(_ < _)
            .foldLeft(("", ""))((valueTuple, location) => {
              val dotIndex = location.indexOf('.')
              ((valueTuple._1.concat(location.substring(0, dotIndex + 2)),
                valueTuple._2.concat(location.substring(4).concat(" "))))
            })
          val user = x(3).replaceAll("[\\[\\]]", "").split(",")(2).split("#")(1)
          val gpsTupleTmp = locationInfo._2.split(" ")
          (user, (locationInfo._1, (gpsTupleTmp(0).toDouble, gpsTupleTmp(1).toDouble)))
        } catch {
          case _ => ("", ("", (0D, 0D)))
        }
      })
      .filter(x => x != ("", ("", (0D, 0D))))
      .join(recentlyActiveUser)
      .map(x => {
        (x._2._1._1, (x._1, x._2._1._2))
      })
      .groupByKey()
      .flatMap(x => {
        val userInfo = if (x._2.size >= SAMPLE_THRESHOLD) {
          knuthShuffle(x._2.toArray)
        } else x._2.toArray
        val userNumber = userInfo.size
        val userDistance = new ArrayBuffer[(String, (String, Double))](userNumber * userNumber)
        for(i <- 0 until userNumber) {
          for (j <- 0 until userNumber) {
            if (i != j) {
              userDistance.append((userInfo(i)._1, (userInfo(j)._1, distance(userInfo(i)._2, userInfo(j)._2))))
            }
          }
        }
        userDistance.filter(x => x != null && x._1 != "" && x._2._1 != "" && x._2._2 < DISTANCE_THRESHOLD)
          .map(x => (x._1, x._2._1)).distinct.toSeq
      })

    val userCard = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(x => x.length == 3 && x(0).toInt >= REAL_USER_ID_THRESHOLD && x(2) == "card") //card
      .filter(_(0).toInt >= REAL_USER_ID_THRESHOLD)
      .map(x => (x(1), x(0))) // card, owner

    val cardLikedNumber = sc.textFile(args(3))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(1), 1))
      .reduceByKey((a, b) => a + b) // card, cardLikedNumber

    val userFollowingSet = sc.textFile(args(5))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => (x(0).toInt >= REAL_USER_ID_THRESHOLD && x(1).toInt >= REAL_USER_ID_THRESHOLD))
      .map(x => (x(1), x(0)))
      .join(recentlyActiveUser)
      .map(x => (x._1, x._2._1))
      .groupByKey()
      .map(x => (x._1, x._2.toSet)) //user, FollowingSet

    val result = userCard.join(cardLikedNumber) //card, (owner, cardLikedNumber)
      .map(x => (x._2._1, (x._1, x._2._2))) //owner, (card, cardLikedNumber)
      .join(userNearby) // (nearbyUser, ((nearbyUserCard, cardLikedNumber), user))
      .map(x => (x._2._2, (x._1, x._2._1))) // (user, (nearbyUser, (nearbyUserCard, cardLikedNumber)))
      .groupByKey()
      .join(userFollowingSet) // user, followingSet
      .map(x => {
      val user = x._1
      val followingSet = x._2._2
      val candidates = x._2._1.filter(y => !followingSet(y._1)).map(_._2).toSeq.sortWith(_._2 > _._2)
        .map(_._1)
        .distinct
        .map(_ + ":" + DISTANCE_THRESHOLD.toString)
        .take(CANDIDATES_SIZE)
      (user, candidates)
    })
    result.saveAsTextFile(args(6))
    result.map(x => NearByCandidates(x._1, x._2)).saveToEs("recommendation/nearby_card")
  }
}
