package com.feel.recommend

/**
 * Created by canoe on 7/11/15.
 */


import breeze.linalg.min
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.elasticsearch.spark._
import scala.collection.immutable.HashSet
import scala.util.Random.nextInt

case class CCUserRecommend(user: String, candidates: Seq[String])

object RecommendUserBasedOnCCFollowing {


  private val REAL_USER_ID_BOUND = 1075
  private var FOLLOWER_NUMBER_UP_BOUND = 0
  private var FOLLOWER_NUMBER_BOTTOM_BOUND = 0
  private val FOLLOWING_NUMBER_THRESHOLD = 5000
  private var CANDIDATES_SIZE = 200
  private val RDD_PARTITION_SIZE = 100
  private val CC_SIZE_THRESHOLD = 1000

  def knuthShuffle[T](x: Array[T]) = {
    for (i <- (1 until x.length).reverse) {
      val j = nextInt(i + 1)
      val tmp = x(i)
      x(i) = x(j)
      x(j) = tmp
    }
    x
  }


  def main(args: Array[String]) = {

    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))

    val sc = new SparkContext(conf)

    FOLLOWER_NUMBER_BOTTOM_BOUND = args(4).toInt
    FOLLOWER_NUMBER_UP_BOUND = args(5).toInt
    CANDIDATES_SIZE = args(6).toInt

    val followerNumber = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), 1))
      .reduceByKey((a, b) => a + b) // follower number

    val rawRDD = sc.textFile(args(1))
      .distinct(RDD_PARTITION_SIZE)
      .map(row =>
      row.split("\t")
      ).filter(_.length == 2)
      .filter(x => x(0).toLong >= REAL_USER_ID_BOUND && x(1).toLong >= REAL_USER_ID_BOUND)

    val RDDA = rawRDD.map(x => (x(0) + "\t" + x(1), "A"))
    val RDDB = rawRDD.map(x => (x(1) + "\t" + x(0), "B"))
    val allRDD = RDDA.join(RDDB)

    val edgeRDD = allRDD
      .distinct(RDD_PARTITION_SIZE)
      .map(x => x._1.split("\t"))
      .map(x => new Edge(x(1).toLong, x(0).toLong, 1L))

    val graph = Graph.fromEdges(edgeRDD, None)
    val cc = graph.connectedComponents()

    val followRDD = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1)))
      .join(followerNumber) // leader, follower, followerNumber
      .map(x => (x._2._1, x._1)) // follower, leader
      .reduceByKey((a, b) => a + "\t" + b)
      .map(x => (x._1, x._2.split("\t").toSeq)) // follower, following List

    val filteredFollowRDD = followRDD.filter(_._2.length <= FOLLOWING_NUMBER_THRESHOLD)

    val userDislikeSet = sc.textFile(args(7))
      .map(_.split("\t"))
      .map(x => (x(0), x(1)))
      .groupByKey()
      .map(x => (x._1, x._2.toSet))

    val recommendCandidates = cc.vertices.map(x => (x._2.toString, x._1.toString)).reduceByKey((a, b) => a + "\t" + b)
      .map(x => x._2.split("\t"))
      .flatMap(x => {
      val length = x.length
      if (length >= CC_SIZE_THRESHOLD) {
        val number = min(length / CC_SIZE_THRESHOLD, 20)
        val result = new Array[(String, String)](number * CC_SIZE_THRESHOLD * CC_SIZE_THRESHOLD)
        for (it <- 0 until number) {
          val tmpX = knuthShuffle(x).take(CC_SIZE_THRESHOLD)
          for (i <- 0 until CC_SIZE_THRESHOLD) {
            for (j <- 0 until CC_SIZE_THRESHOLD) {
              result(it * CC_SIZE_THRESHOLD * CC_SIZE_THRESHOLD +  i * CC_SIZE_THRESHOLD + j) = (tmpX(i), tmpX(j))
            }
          }
        }
        result.filter(_ != null).toSeq
      } else {
        val result = new Array[(String, String)](length * length)
        for (i <- 0 until length) {
          for (j <- 0 until length) {
            result(i * length + j) = (x(i), x(j))
          }
        }
        result.filter(_ != null).toSeq
      }
    })
      .distinct(RDD_PARTITION_SIZE)
      .join(filteredFollowRDD) // a, b, recommended raw followings
      .map(x => (x._2._1, x._2._2)) // b, recommended raw followings
      .reduceByKey((a, b) => a ++ b) // b recommended raw followings
      .join(followRDD) // b, b recommended raw followings, b following
      .leftOuterJoin(userDislikeSet)
      .map(x => {
      val dislikeSet = x._2._2 match {
        case Some(set) => set
        case None => new HashSet[String]()
      }
      val followSet = x._2._1._2.toSet // following set
      val candidates = x._2._1._1.filter(y => y != x._1 && !dislikeSet(y) && !followSet(y)).distinct // filtered
      // recommends
      (x._1, candidates) // b, candidates
    })
      .filter(_._2.length != 0)
      .flatMap(x => x._2.map(y => (y, x._1))) // candidate, b


    val recentlyActiveUser = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(_(0))
      .filter(_.toInt >= REAL_USER_ID_BOUND)
      .distinct(RDD_PARTITION_SIZE)
      .map(x => (x, "_")) // recently active user

    val result = recommendCandidates.join(followerNumber)
      .map(x => (x._1, (x._2._1, x._2._2))) // candidate, (b, c's follower number)
      .join(recentlyActiveUser) //recently active candidate, ((b, c's follower number), "_")
      .map(x => (x._2._1._1, x._1 + "," + x._2._1._2.toString)) // b, r a candidate, c's follower number
      .reduceByKey((a, b) => a + "\t" + b)
      .map(x => {
      val user = x._1
      val candidates = x._2.split("\t").map(_.split(","))
        .sortWith(_(1).toInt > _(1).toInt).map(_(0)).distinct.take(CANDIDATES_SIZE).toSeq.mkString(",")
      (user, candidates)
    })
    //result.saveToEs("recommendation/CCFollowing")
    result.saveAsTextFile(args(3))

  }
}
