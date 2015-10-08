package com.feel.statistics

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by aidi.feng on 15/9/28.
 */
object MergeResultGender {

  def main (args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext()

    val active1 = sc.textFile(args(0))
    .map(_.split("\t"))
    .filter(_.length == 3)
    .map(x => (x(0), (x(1), x(2)))) //gender, (number, ratio)

    val active2 = sc.textFile(args(1))
    .map(_.split("\t"))
    .filter(_.length == 3)
    .map(x => (x(0), (x(1), x(2))))

    val active3 = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .map(x => (x(0), (x(1), x(2))))

    val gender = sc.textFile(args(3))
    .map(_.split("\t"))
    .filter(_.length == 3)
    .map(x => (x(0), (x(1), x(2)))) // gender, (total_number, ratio)

    val nostop = sc.textFile(args(5))
    .map(_.split("\t"))
    .filter(_.length == 3)
    .map(x => (x(0), (x(1), x(2)))) // gender, (nostop_number, ratio)

    val result = active1.join(active2) // gender, ((active1number, ratio), (active2number, ratio))
    .join(active3) // gender, (((active1number, ratio), (active2number, ratio)), (active3number, ratio))
    .join(gender) // gender, ((((active1number, ratio), (active2number, ratio)), (active3number, ratio)), (number, ratio))
    .join(nostop) // gender, (((((active1number, ratio), (active2number, ratio)), (active3number, ratio)), (number, ratio)), (nostopNumber, ratio))
    .map({case(gender, (((((a1num, r1), (a2num, r2)), (a3num, r3)), (number, r)), (nonum, r0))) =>
      "性别:" + gender + "\t注册数:" + number + "\t占比:" + r +
        "\t登录1天:" + a1num + "\t占比:" + r1 +
        "\t登录2天:" + a2num + "\t占比:" + r2 +
        "\t登录3天:" + a3num + "\t占比:" + r3 +
        "\t" + nonum + "\t" + r0
    })
    result.saveAsTextFile(args(4))
  }
}
