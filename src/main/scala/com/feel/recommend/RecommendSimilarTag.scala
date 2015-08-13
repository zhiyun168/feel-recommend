package com.feel.recommend

import breeze.linalg.min
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer

/**
 * Created by canoe on 8/12/15.
 */
object RecommendSimilarTag {

  private val TAG_USED_NUMBER_FILTER = 100
  private val CON_EXIST_TAG = 10
  private val INF = (1 << 30)

  def jaccardSimilarity(A: String, B: String) = {
    val aSet = A.toSet
    val bSet = B.toSet
    ((aSet & bSet).size + 0D) / ((aSet | bSet).size + 0D)
  }

  def editDistance(A: String, B: String) = {
    val dp = Array.ofDim[Int](A.length + 1, B.length + 1)
    for (i <- 0 to A.length) {
      for (j <- 0 to B.length) {
        if (i == 0)
          dp(i)(j) = j
        else if (j == 0)
          dp(i)(j) = i
        else
          dp(i)(j) = INF
      }
    }
    for (i <- 1 to A.length) {
      for (j <- 1 to B.length) {
        if (A(i - 1) == B(j - 1))
          dp(i)(j) = dp(i - 1)(j - 1)
        else {
          dp(i)(j) = min(dp(i)(j), dp(i - 1)(j - 1) + 1)
          dp(i)(j) = min(dp(i)(j), dp(i - 1)(j) + 1)
          dp(i)(j) = min(dp(i)(j), dp(i)(j - 1) + 1)
        }
      }
    }
    dp(A.length)(B.length)
  }

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val tagInfoRDD = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 3)

    val coExitTagRDD = tagInfoRDD.map(x => (x(0), (x(1), x(2)))) //card, bid, bname
      .groupByKey()
      .flatMap(x => {
      val coExistTag = new ArrayBuffer[((String, String), (String, String))]()
      for (i <- x._2) {
        for(j <- x._2) {
          if (i != j)
            coExistTag.append((i, j))
        }
      }
      coExistTag.map(x => (x, 1)).toSeq
    })
      .reduceByKey((a, b) => a + b)
      .map(x => (x._1._1, (x._1._2, x._2)))
      .groupByKey()
      .map(x => {
      val tag = x._1
      val sortedCoExitTag = x._2.toSeq.sortWith(_._2 > _._2).take(CON_EXIST_TAG)
      (tag, sortedCoExitTag)
    })
    coExitTagRDD.saveAsTextFile(args(1))

    val topTagRDD = tagInfoRDD.map(x => ((x(1), x(2)), 1))
      .reduceByKey((a, b) => a + b)
      .filter(_._2 > TAG_USED_NUMBER_FILTER)
      .map(_._1)


    def topTagSimilarity(data: RDD[((String, String), (String, String))], f: (String, String) => Double) = {
      data.map(x => (x._1, (x._2, f(x._1._2, x._2._2))))
        .groupByKey()
        .map(x => {
        (x._1, x._2.toSeq.sortWith(_._2 < _._2).mkString("\\|"))
      })
    }

    val topTagCartesianRDD = topTagRDD.cartesian(topTagRDD)
      .filter(x => x._1 != x._2)

    topTagSimilarity(topTagCartesianRDD, editDistance).saveAsTextFile(args(2))
    topTagSimilarity(topTagCartesianRDD, jaccardSimilarity).saveAsTextFile(args(3))
  }
}
