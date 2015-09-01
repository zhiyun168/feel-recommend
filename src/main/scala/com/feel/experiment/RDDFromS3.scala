package com.feel.experiment

import org.apache.spark.{SparkContext, SparkConf}
import scala.util.parsing.json.JSON

/**
 * Created by canoe on 9/1/15.
 */
object RDDFromS3 {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val inputPathPrefix = "s3://feel.recommender.log/recommend-middleman/"
    val date = args(0)
    val rdd = sc.textFile(inputPathPrefix + date + "/").map(x => {
      val xMap = JSON.parseFull(x)
      xMap match {
        case Some(map: Map[String, Any]) => {
          map("id") + "\t" + map("action") + "\t" + map("candidates")
        }
        case _ => "?"
      }
    }).filter(_ != "?")

    rdd.saveAsTextFile(args(1))
  }
}
