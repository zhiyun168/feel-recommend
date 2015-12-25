package com.feel.statistics

import org.apache.spark.SparkContext

/**
  * Created by canoe on 12/25/15.
  */
object NginxLog {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val logAPIAccessNumber = sc.textFile(args(0))
      .map(_.split(" "))
      .filter(_.length >= 7)
      .map(x => try (
        ((x(5).tail,
        {
          val api = if (x(6).contains("?")) {
            x(6).split("\\?")(0)
          } else {
            x(6)
          }
          def replaceStr(str: String) = {
            if (str.length != 0 && str.foldLeft(0)((acc, value) =>
              acc + { if (value >= '0' && value <= '9') 1 else 0 }
            ) == str.length)  "id"
            else str
          }
          api.split("\\/").map(replaceStr(_)).mkString("/")
        }), 1)
        )).reduceByKey((a, b) => a + b)
    .sortBy(_._2)
    .map(x => x._1._1 + "\t" + x._1._2 + "\t" + x._2)
    logAPIAccessNumber.saveAsTextFile(args(1))
  }
}
