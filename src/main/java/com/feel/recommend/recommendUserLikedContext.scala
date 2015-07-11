package com.feel.recommend

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
 * Created by canoe on 6/27/15.
 * not using
 */

case class contextRecommend(user: String, contextCandidates: Seq[String])

object recommendUserLikedContext {

  private val REAL_USER_ID_BOUND = 1075

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))
    val sc = new SparkContext(conf)

    val userContext = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 3) // user, ucontext
      .filter(_(0).toLong >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1)))
      .reduceByKey((a, b) => a + "\t" + b)

    val recommendedContext = sc.textFile(args(2))
      .map(_.replaceAll("[A-Za-z()]", "").split(","))
      .filter(_.length > 2)
      .map(x => (x(0), x.tail))
      .flatMap(x => x._2.map(y => (y, x._1))) //ruser, user
      .join(userContext) // ruser, user, rcontext
      .map(x => (x._2._1, (x._1, x._2._2))) // user, ruser, rcontext
      .groupByKey(100)
      .join(userContext) // user, [ruser, rcontext], ucontext
      .map(x => {
      val user = x._1
      val userContextSet = x._2._2.split("\t").toSet
      val contextCandidates = x._2._1.map(_._2.split("\t")).flatten.filter(x => !userContextSet(x)).toSet.take(100).toSeq
      (user, contextCandidates)
    })
    recommendedContext.saveToEs("recommendation/alsoFollowingJoinedTask")
    recommendedContext.saveAsTextFile(args(3))
  }
}