package com.feel.statistics

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by aidi.feng on 15/9/28.
 */
object MergeResultTotal {

  def main (args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext()

    val totalUser = sc.textFile(args(0)).map(_.split("\t"))
    .filter(_.length == 1).map(x =>  x(0)).first().toInt
    //println("!!!!!!" + totalUser)

    val active1 = sc.textFile(args(1))
    .map(_.split("\t"))
    .filter(_.length == 2)
    .map(x => (totalUser, (x(0), x(1)))) // number, (activeNumber, ratio)

    val active2 = sc.textFile(args(2))
    .map(_.split("\t"))
    .filter(_.length == 2)
    .map(x => (totalUser, (x(0), x(1))))

    val active3 = sc.textFile(args(3))
    .map(_.split("\t"))
    .filter(_.length == 2)
    .map(x => (totalUser, (x(0), x(1))))

    val nostop = sc.textFile(args(5))
    .map(_.split("\t"))
    .filter(_.length == 2)
    .map(x => (totalUser, (x(0), x(1)))) // number, (nostopNumber, ratio)

   val result = active1.join(active2) // number, ((active1Number, ratio), (active2Number, ratio))
   .join(active3) // number, (((active1Number, ratio), (active2Number, ratio)), (active3Number, ratio))
   .join(nostop) // number, ((((active1Number, ratio), (active2Number, ratio)), (active3Number, ratio)), (nostopNumber, ratio))
   .map({case(number, ((((a1num, r1), (a2num, r2)), (a3num, r3)), (nonum, r0))) =>
      "总注册数:" + number + "\t登录1天:" + a1num + "\t占比:" + r1 +
        "\t登录2天:" + a2num + "\t占比:" + r2 +
        "\t登录3天:" + a3num + "\t占比:" + r3 +
        "\t" + nonum + "\t" + r0
    })
    result.saveAsTextFile(args(4))

  }
}
