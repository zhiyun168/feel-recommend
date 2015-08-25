package com.feel.recommend

import breeze.numerics.abs
import org.apache.spark.{SparkContext, SparkConf}

import org.elasticsearch.spark._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
 * Created by canoe on 8/3/15.
 */

case class UserRecommend(user: String, candidates: Seq[String])

object RankRecommendedUser {

  private val REAL_ID_BOUND = 1075
  private var CANDIDATES_SIZE = 100
  private val TAG_SIZE = 5
  private var DIFF_GENDER_SCORE = 10D

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))

    val sc = new SparkContext(conf)



    val userGender = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_ID_BOUND)
      .map(x => (x(0), x(1)))

    val userGenderFeature = sc.textFile(args(2))
      .map(_.replaceAll("[()]", "").split(","))
      .map(x => (x(0), x(1)))
      .groupByKey()
      .map(x => {
      val user = x._1
      val featureKeySet = Set("age", "followingRatio", "mostTag")
      val featureHash = x._2.foldLeft(new mutable.HashMap[String, String]())((featureHash, feature) => {
        val tmp = feature.split(":")
        if (featureKeySet(tmp(0))) {
          featureHash(tmp(0)) = tmp(1)
        }
        featureHash
      })
      val featureArray = new ArrayBuffer[String]
      featureArray.append(featureHash.getOrElse("age", "22"))
      featureArray.append(featureHash.getOrElse("followingRatio", "0"))
      featureArray.append(featureHash.getOrElse("mostTag", ""))
      (user, featureArray)
    }).join(userGender)
    .map(x => {
      (x._1 + "\t" + x._2._2, x._2._1)
    })

    val userFollowingAverageFeature = sc.textFile(args(3))
      .map(_.replaceAll("[()]", "").split(","))
      .filter(_.length == 4)
      .map(x => (x.head, x.tail))


    DIFF_GENDER_SCORE = args(6).toDouble
    CANDIDATES_SIZE = args(7).toInt

    val rankedRecommendedUserRDD = sc.textFile(args(4))
      .map(_.replaceAll("[a-zA-z() ]", "").split(","))
      .flatMap(x => {
      val user = x.head
      val candidates = x.tail
      candidates.map(c => (c, user)) // candidate, user
    })
      .join(userGender) // candidate, (user, candidateGender)
      .map(x => {
      (x._1 + "\t" + x._2._2, x._2._1) // candidateGender, user
    })
      .join(userGenderFeature) // (candidateGender, (user, candidateGenderFeature))
      .map(x => (x._2._1, (x._1, x._2._2))) // user, (candidateGender, candidateGenderFeature) = C
      .join(userGender) // user, (C, gender)
      .map(x => (x._1 + "\t" + x._2._2, x._2._1)) // userGender, C
      .join(userFollowingAverageFeature) // userGender, C, userFollowingAverageFeature
      .map(x => {
      val userAverageFeature = x._2._2
      val candidateFeature = x._2._1._2

      val userTmp = x._1.split("\t")
      val candidateTmp = x._2._1._1.split("\t")
      val userGender = userTmp(1)
      val candidateGender = candidateTmp(1)

      val distance = abs(userAverageFeature(0).toDouble - candidateFeature(0).toDouble) * 0.1D +
        abs(userAverageFeature(1).toDouble - candidateFeature(1).toDouble) * 0.5D +
        TAG_SIZE - (userAverageFeature(2).split("\\|").toSet & candidateFeature(2).split("\\|").toSet).size +
        { if (userGender == candidateGender) DIFF_GENDER_SCORE else 0D }

      val user = userTmp(0)
      val candidate = candidateTmp(0)
      (user, (candidate, distance))
    })
    .groupByKey()
    .map(x => {
      val user = x._1
      val candidates = x._2.toArray.sortWith(_._2 < _._2).map(_._1).distinct.take(CANDIDATES_SIZE)
      (user, candidates)
    })
    rankedRecommendedUserRDD.map(x => (x._1, x._2.mkString(","))).saveAsTextFile(args(5))
    rankedRecommendedUserRDD.map(x => UserRecommend(x._1, x._2)).saveToEs("recommendation/rankedUser")
  }
}
