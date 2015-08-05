package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable
import org.elasticsearch.spark._

/**
 * Created by canoe on 8/3/15.
 */
object RecentlyMostlyUsedTag {

  private val REAL_USER_ID_BOUND = 1075
  private val TAG_SIZE = 5

  case class MostTag(user: String, mostTag: String)

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))
    val sc = new SparkContext(conf)

    val systemSet = sc.textFile(args(1))
      .collect()
      .toSet

    val mostTagRDD = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && !systemSet(x(1)))
      .map(x => (x(0), x(1)))
      .groupByKey()
      .map(x => {
      val user = x._1
      val tagCount = new mutable.HashMap[String, Int]()
      x._2.foreach(tag => {
        if (tagCount.get(tag).isEmpty)
          tagCount(tag) = 1
        else
          tagCount(tag) += 1
      })
      val mostTag = tagCount
        .toArray
        .sortWith(_._2 > _._2)
        .take(TAG_SIZE)
        .map(_._1)
        .mkString(",")
      MostTag(user, mostTag)
    })
    mostTagRDD.saveToEs("defaultTag/recentlyMostlyUsed")
    mostTagRDD.saveAsTextFile(args(3))
  }
}

