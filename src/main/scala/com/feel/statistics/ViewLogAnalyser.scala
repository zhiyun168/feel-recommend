package com.feel.statistics

import org.apache.spark.SparkContext
import org.codehaus.jettison.json.JSONObject


/**
  * Created by canoe on 1/21/16.
  */
object ViewLogAnalyser {
  def main(args: Array[String]) = {
    val sc = new SparkContext()
    sc.textFile(args(0))
    .map(x => {
      val json = new JSONObject(x)
      val key = json.get("type").toString + "\t" + json.get("sub_type").toString + "\t" + json.get("action").toString
      (key, 1)
    }).reduceByKey((a, b) => a + b)
    .saveAsTextFile(args(1))
  }
}
