package com.feel.statistics

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by canoe on 9/18/15.
 */
object ShareInfo {

  private val DATA_NAME = List("分享总数", "有分享行为用户数", "有分享行为今日新用户数", "有分享行为用户人均分享数",
  "有分享行为今日新用户人均分享数", "类型分享数", "类型分享用户数")

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val newUser = sc.textFile(args(0))
      .map(x => (x, "_"))
      .distinct()

    val shareInfo = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1))) //card, user

    val sharedTotalNumber = shareInfo.map(_._1).count()

    val userHasSharedNumber = shareInfo.map(_._2).distinct().count()

    val newUserHasSharedNumber = shareInfo.map(x => (x._2, "_"))
      .distinct()
      .join(newUser)
      .map(_._1)
      .count()

    val userAverageShareNumber = shareInfo.map(x => (x._2, 1))
      .reduceByKey((a, b) => a + b)
      .map(_._2)
      .mean()

    val newUserAverageShareNumber = shareInfo.map(x => (x._2, 1))
      .join(newUser)
      .map(x => (x._1, x._2._1))
      .reduceByKey((a, b) => a + b)
      .map(_._2)
      .mean()

    val sharedTypeInfo = shareInfo.map(x => {
      val sharedType = if (x._1.contains("goal")) "goal"
        else if (x._1.contains("card")) "card"
        else ""
      (sharedType, x._2)
      }).filter(_._1 != "")

    val sharedTypeNumberInfo = sharedTypeInfo
      .map(x => (x._1, 1))
      .reduceByKey((a, b) => a + b)
      .collect()
      .mkString(";")

    val sharedTypeUserNumber = sharedTypeInfo
      .distinct()
      .map(x => (x._1, 1))
      .reduceByKey((a, b) => a + b)
      .collect()
      .mkString(";")

    val dataValue = List(sharedTotalNumber, userHasSharedNumber, newUserHasSharedNumber, userAverageShareNumber,
    newUserAverageShareNumber, sharedTypeNumberInfo, sharedTypeUserNumber)

    val resultRDD = sc.parallelize(DATA_NAME.zip(dataValue))
    resultRDD.saveAsTextFile(args(2))

  }
}
