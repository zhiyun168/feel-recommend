package com.feel.recommend

/**
 * Created by canoe on 7/11/15.
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.elasticsearch.spark._

case class ccUserRecommend(user: String, candidates: Seq[String])

object recommendUserBasedOnCCFollowing {


  private val REAL_USER_ID_BOUND = 1075
  private var FOLLOWER_NUMBER_UP_BOUND = 0
  private var FOLLOWER_NUMBER_BOTTOM_BOUND = 0
  private val CANDIDATES_SIZE = 100
  private val RDD_PARTITION_SIZE = 10
  private val CC_PARTITION_SIZE = 100
  private val CC_SIZE_THRESHOLD = 1000

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))

    val sc = new SparkContext(conf)

    val rawRDD = sc.textFile(args(1))
      .distinct(RDD_PARTITION_SIZE)
      .map(row =>
      row.split("\t")
      ).filter(_.length == 3)
      .filter(x => x(0).toLong >= REAL_USER_ID_BOUND && x(1).toLong >= REAL_USER_ID_BOUND)

    val RDDA = rawRDD.map(x => (x(0) + "\t" + x(1), "A"))
    val RDDB = rawRDD.map(x => (x(1) + "\t" + x(0), "B"))
    val allRDD = RDDA.join(RDDB)

    val edgeRDD = allRDD
      .distinct(RDD_PARTITION_SIZE)
      .map(x => x._1.split("\t"))
      .map(x => new Edge(x(0).toLong, x(1).toLong, 1L))

    val graph = Graph.fromEdges(edgeRDD, None)
    val cc = graph.connectedComponents()

    val followRDD = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(1), x(0)))
      .reduceByKey((a, b) => a + "\t" + b)

    val recommendCandidates = cc.vertices.map(x => (x._2.toString, x._1.toString)).reduceByKey((a, b) => a + "\t" + b)
      .map(x => x._2.split("\t"))
      .flatMap(x => {
      val length = x.length
      if (length >= CC_SIZE_THRESHOLD) {
        val partitionSize = CC_PARTITION_SIZE
        val partitionNumber = length / partitionSize
        val result = new Array[(String, String)](partitionNumber * partitionSize * partitionSize)
        for (i <- 0 until partitionNumber) {
          val resultStartIndex = i * partitionSize * partitionSize
          val xStartIndex = i * partitionSize
          for (j <- 0 until partitionSize) {
            for (k <- 0 until partitionSize) {
              if (j != k) {
                result(resultStartIndex + j * partitionSize + k) = (x(xStartIndex + j), x(xStartIndex + k))
              }
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
      .join(followRDD) // a, b, followings
      .map(x => (x._2._1, x._2._2)) // b, followings
      .reduceByKey((a, b) => a + "\t" + b) // b recommended raw followings
      .join(followRDD) // b, b following, b recommended raw followings
      .map(x => {
      val followSet = x._2._2.split("\t").toSet // following set
      val candidates = x._2._1.split("\t").filter(y => y != x._1 && !followSet(y)).distinct // filtered recommends
      (x._1, candidates) // b, candidates
    }).filter(_._2.length != 0)
      .flatMap(x => x._2.map(y => (y, x._1))) // b, candidate

    FOLLOWER_NUMBER_BOTTOM_BOUND = args(4).toInt
    FOLLOWER_NUMBER_UP_BOUND = args(5).toInt

    val followerNumber = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), 1))
      .reduceByKey(_ + _) // follower number
      .filter(x => x._2 <= FOLLOWER_NUMBER_UP_BOUND && x._2 >= FOLLOWER_NUMBER_BOTTOM_BOUND)

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
      val candidates = x._2.split("\t").map(_.split(",")).sortWith(_(1).toInt > _(1).toInt).map(_(0)).take(CANDIDATES_SIZE).toSeq
      ccUserRecommend(user, candidates)
    })
      result.saveToEs("recommendation/CCFollowing")
      result.saveAsTextFile(args(3))
  }

}
