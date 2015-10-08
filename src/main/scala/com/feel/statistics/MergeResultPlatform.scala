package com.feel.statistics

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by feel on 15/9/28.
 */
object MergeResultPlatform {

  def main (args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext()

    val platform = sc.textFile(args(0))
    .map(_.split("\t"))
    .filter(_.length == 3)
    .map(x => (x(0), (x(1), x(2)))) // platform, (totalnumber, ratio)

    val active1 = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .map(x => (x(0), (x(1), x(2)))) //platform, (number, ratio)

    val active2 = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .map(x => (x(0), (x(1), x(2)))) //platform, (number, ratio)

    val active3 = sc.textFile(args(3))
        .map(_.split("\t"))
        .filter(_.length == 3)
        .map(x => (x(0), (x(1), x(2)))) //platform, (number, ratio)

    val nostop = sc.textFile(args(5))
    .map(_.split("\t"))
    .filter(_.length == 3)
    .map(x => (x(0), (x(1), x(2)))) // platform. (nostopnum, ratio)

    val result = platform.join(active1) // platfrom, ((number, ratio), (active1number, ratio))
    .join(active2) // platfrom, (((number, ratio), (active1number, ratio)), (active2number, ratio))
    .join(active3) // platfrom, ((((number, ratio), (active1number, ratio)), (active2number, ratio)), (active3number, ratio))
    .join(nostop) // platfrom, (((((number, ratio), (active1number, ratio)), (active2number, ratio)), (active3number, ratio)), (nostopNumber, ratio))
    .map({case(platform, (((((number, r), (a1num, r1)), (a2num, r2)), (a3num, r3)), (nonum, r0))) =>
      platform + "\t注册数:" + number + "\t占比:" + r +
        "\t登录1天:" + a1num + "\t占比:" + r1 +
        "\t登录2天:" + a2num + "\t占比:" + r2 +
        "\t登录3天:" + a3num + "\t占比:" + r3 +
        "\t" + nonum + "\t" + r0
    })
    result.saveAsTextFile(args(4))
  }
}
