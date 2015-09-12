package com.feel.statistics
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by aidi.feng on 15/9/9.
 */
object NewUserRegister {

  private val STAMP_PER_DAY = 86400000

  def main (args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val DateFormat = new SimpleDateFormat("yyyy/MM/dd")
    val currentTime = System.currentTimeMillis()
    val lastDayTime = currentTime - STAMP_PER_DAY
    val Date = DateFormat.format(lastDayTime)

    val RegisterGender = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(1), 1)) //gender

    val newUser = RegisterGender.count()

    val lastDayRegisterUser = RegisterGender.map(x => (x._2, 1))
      .reduceByKey((a, b) => a + b)
      .map(x => x._2)
    lastDayRegisterUser.saveAsTextFile(args(1))


    val GenderResult = RegisterGender.reduceByKey((a, b) => a + b)
      .map(x => x._1 + "\t" + x._2 + "\t" + (x._2.toDouble / newUser) )
    GenderResult.saveAsTextFile(args(2))

    val android = "[a-zA-Z0-9_]+android[_a-zA-z0-9]*"
    val ios = "[a-zA-Z0-9_]+ios"

    val RegisterPlatform = sc.textFile(args(0))
      .map(_.replaceAll(android, "android"))
      .map(_.replaceAll(ios, "ios"))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), 1)) //platform
      .map(x => {
        val p  = if (x._1 != "android" && x._1 != "ios") "unknown" else x._1
        (p, x._2)
      })
      .reduceByKey((a, b) => a + b)
      .map(x => x._1 + "\t" + x._2 + "\t" + (x._2.toDouble / newUser))
    RegisterPlatform.saveAsTextFile(args(3))

  }

}
