package com.feel.statistics

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by aidi.feng on 15/9/28.
 */
object MergeResultGenderPlatform {

  def main (args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext()

    val total = sc.textFile(args(0))
    .map(_.split("\t"))
    .filter(_.length == 1)
    .map(x => x(0)).first().toInt

    val RDD = sc.textFile(args(1))
    .map(_.split("\t"))
    .filter(_.length == 9)
    .map(x => (x(0), x(1))) // gender&platform, totalnumber
    .map(x => {
      val tmp = x._2.split(":")(1).toInt
      (x._1, (x._2, "%.2f".format(tmp.toDouble / total * 100)))
    })

    val active1 = sc.textFile(args(2))
    .map(_.split("\t"))
    .filter(_.length == 3)
    .map(x => (x(0), (x(1), x(2)))) //gender&platform, (number, ratio)

    val active2 = sc.textFile(args(3))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .map(x => (x(0), (x(1), x(2)))) //gender&platform, (number, ratio)

    val active3 = sc.textFile(args(4))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .map(x => (x(0), (x(1), x(2)))) //gender&platform, (number, ratio)

    val nostop = sc.textFile(args(6))
    .map(_.split("\t"))
    .filter(_.length == 3)
    .map(x => (x(0), (x(1), x(2)))) // gender&platform, (nostop_number, ratio)

    val result = RDD.join(active1) // gender&platform, ((number, ratio), (active1number, ratio))
    .join(active2) // gender&platform, (((number, ratio), (active1number, ratio)), (active2number, ratio))
    .join(active3) //// gender&platform, ((((number, ratio), (active1number, ratio)), (active2number, ratio)), (active3number, ratio))
    .join(nostop) //// gender&platform, (((((number, ratio), (active1number, ratio)), (active2number, ratio)), (active3number, ratio)), (nostopNumber, ratio))
    .map({case(gp, (((((number, r), (a1num, r1)), (a2num, r2)), (a3num, r3)), (nonum, r0))) =>
      gp + "\t" + number + "\t占比:" + r + "%" +
        "\t登录1天:" + a1num + "\t占比:" + r1 +
        "\t登录2天:" + a2num + "\t占比:" + r2 +
        "\t登录3天:" + a3num + "\t占比:" + r3 +
        "\t" + nonum + "\t" + r0
    })
    result.saveAsTextFile(args(5))
  }

}
