package com.feel.statistics

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by aidi.feng on 15/9/11.
 */
object ActiveNewUserOf3Days {

  def main (args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val activeUser = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), 1))

    val one = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 1)
      .map(x => (x(0), 1))
      .join(activeUser) //user, (1, 1)
      .count()

    val register1 = sc.textFile(args(7))
      .map(_.split("\t"))
      .filter(_.length == 1)
      .map(x => x(0)).first().toInt

    val result1 = sc.parallelize(List(one, one.toDouble / register1))
    result1.saveAsTextFile(args(4))

    val two = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 1)
      .map(x => (x(0), 1))
      .join(activeUser)
      .count()

    val register2 = sc.textFile(args(8))
      .map(_.split("\t"))
      .filter(_.length == 1)
      .map(x => x(0)).first().toInt

    val result2 = sc.parallelize(List(two, two.toDouble / register2))
    result2.saveAsTextFile(args(5))

    val three = sc.textFile(args(3))
      .map(_.split("\t"))
      .filter(_.length == 1)
      .map(x => (x(0), 1))
      .join(activeUser)
      .count()

    val register3 = sc.textFile(args(9))
      .map(_.split("\t"))
      .filter(_.length == 1)
      .map(x => x(0)).first().toInt

    val result3 = sc.parallelize(List(three, three.toDouble / register3))
    result3.saveAsTextFile(args(6))



  }
}
