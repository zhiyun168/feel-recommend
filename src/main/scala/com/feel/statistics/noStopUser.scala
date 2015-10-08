package com.feel.statistics

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by aidi.feng on 15/9/29.
 */
object noStopUser {

  val android = "[a-zA-Z0-9_]+android[_a-zA-z0-9]*"
  val ios = "[a-zA-Z0-9_]+ios"

  def main (args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext()

    val activeUser = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), 1)) // user, 1

    val total = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 1)
      .map(x => x(0)).first().toInt

    val registerUser = sc.textFile(args(2))
      .map(_.replaceAll("x", "f"))
      .map(_.replaceAll(android, "android"))
      .map(_.replaceAll(ios, "ios"))
      .map(_.split("\t"))
      .filter(_.length == 3) //user, gender, platform
      .map(x => {
      val p  = if (x(2) != "android" && x(2) != "ios") "ios" else x(2)
      (x(0), x(1), p)
    })

    val noStopTotal = registerUser.map(x => (x._1, 1))
      .join(activeUser) // user, (1, 1)
      .count().toInt

    val totalRes = sc.parallelize(List("停留为0的人数:" + (total.toInt - noStopTotal) + "\t占比:" + "%.2f".format((total.toInt - noStopTotal).toDouble / total * 100) + "%"))
    totalRes.saveAsTextFile(args(6))

    val gender = sc.textFile(args(3))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .map(x => (x(0), x(1))) // gender, totalnumber

    val noStopGender = registerUser.map(x => (x._1, x._2))
      .join(activeUser) // user, (gender, 1)
      .map(x => (x._2._1, 1))
      .reduceByKey((a, b) => a + b) //gender, number
      .join(gender) // gender, (number, totalnumber)
      .map(x => {
      val t = x._2._2.toInt - x._2._1
      x._1 + "\t停留为0用户数:" + t + "\t占比:" + "%.2f".format(t.toDouble / x._2._2.toDouble * 100) + "%"
    })
    noStopGender.saveAsTextFile(args(7))

    val platform = sc.textFile(args(4))
    .map(_.split("\t"))
    .filter(_.length == 3)
    .map(x => (x(0), x(1))) // platfrom, totalnumber

    val noStopPlatform = registerUser.map(x => (x._1, x._3))
    .join(activeUser) // user, (platform, 1)
    .map(x => (x._2._1, 1))
    .reduceByKey((a, b) => a + b) // platform, number
    .join(platform) // platform, (number, totalnumber)
    .map(x =>{
      val t = x._2._2.toInt - x._2._1
      x._1 + "\t停留为0用户数:" + t + "\t占比:" + "%.2f".format(t.toDouble / x._2._2.toDouble * 100) + "%"})
    noStopPlatform.saveAsTextFile(args(8))

    val genderPlatform = sc.textFile(args(5))
    .map(_.split("\t"))
    .filter(_.length == 9)
    .map(x => (x(0), x(1))) //gender&platfrom, number:num
    .map({case(gp, num) =>
        val tmp = gp.split(" & ")
        val g = tmp(0)
        val p = tmp(1)
        val number = num.split(":")(1)
        ((g, p), number) //(gender, platfrom), number
    })

    val noStopGP = registerUser.map(x => (x._1, (x._2, x._3))) //user, (gender, platfrom)
    .join(activeUser) // user, ((gender, paltform), 1)
    .map(x => (x._2._1, 1))
    .reduceByKey((a, b) => a + b) // (gender, platform), number
    .join(genderPlatform) // (gender, platform), (number, totalnumber)
    .map({case((g, p), (num, tot)) =>
      val t = tot.toInt - num
      g + " & " + p + "\t停留为0用户数:" + t + "\t占比:" + "%.2f".format(t.toDouble / tot.toDouble * 100) + "%"
    })
    noStopGP.saveAsTextFile(args(9))
  }
}
