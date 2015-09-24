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

  case class MostTag(user: String, candidates: Seq[String])

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))
    val sc = new SparkContext(conf)

    val cardTag = sc.textFile(args(1)) //card, type
      .map(_.split("\t"))
      .filter(x => x.length == 2 && x(1) == "card")
      .map(x => (x(0), x(1)))

    val mostTagRDD = sc.textFile(args(2))
      .map(_.split("\t")) // card, user, tag
      .filter(x => x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), (x(1), x(2))))
      .join(cardTag) // (card, (user, tag), type)
      .map(x => x._2._1) //card, tag
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
      MostTag(user, mostTag)
    })
    mostTagRDD.saveToEs("recommendation_tag/recentlyMostlyUsed")
    mostTagRDD.saveAsTextFile(args(3))
  }
}

