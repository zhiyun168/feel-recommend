package com.feel.experiment

/**
 * Created by canoe on 8/31/15.
 */

import org.elasticsearch.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


case class User(leader: String, follower: String)

object RDDFromES {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    val scRDD = sc.textFile(args(1)).map(_.split("\t")).map(x => (x(0), x(1)))
    val esRDD = sql.read.format("org.elasticsearch.spark.sql").load("recommendation/USER")
    esRDD.map(x => (x(1).toString, x(0).toString))
      .join(scRDD)
      .saveAsTextFile(args(2))
  }
}
