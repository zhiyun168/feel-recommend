package com.feel.statistics

import org.apache.spark.SparkContext

/**
  * Created by canoe on 1/20/16.
  */
object AllReport {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val data = sc.textFile(args(0))
      data.map(_.split("\t"))
          .filter(_.length == 2)
          .map(x => (x(0), x(1)))
          .groupByKey()
          .map(x => "uid:" + x._1 + ";" + x._2.toSeq.mkString(","))
          .saveAsTextFile(args(1))
  }
}
