package com.feel.statistics

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by aidi.feng on 15/9/11.
 */
object ActiveNewUserOf3Days {


  val android = "[a-zA-Z0-9_]+android[_a-zA-z0-9]*"
  val ios = "[a-zA-Z0-9_]+ios"

  def main (args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext()

    val activeUser = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), 1)) // user, 1

    for (i <- 1 to 3) {
      val day0 = sc.textFile(args(i))
        .map(_.replaceAll("x", "f"))
        .map(_.replaceAll(android, "android"))
        .map(_.replaceAll(ios, "ios"))
        .map(_.split("\t"))
        .filter(_.length == 3) //user, gender, platform
        .map(x => {
        val p  = if (x(2) != "android" && x(2) != "ios") "ios" else x(2)
        (x(0), x(1), p)
      })


      val day = day0.map(x => (x._1, 1))
        .join(activeUser) //user, (1, 1)
        .count()

      val register = sc.textFile(args(i + 6))
        .map(_.split("\t"))
        .filter(_.length == 1)
        .map(x => x(0)).first().toInt//total register

      val result = sc.parallelize(List(day.toInt + "\t" + "%.2f".format(day.toDouble / register * 100) + "%"))
      result.saveAsTextFile(args(i + 3))


      val dayPlat = day0.map(x => (x._1, x._3)) //user, platform
        .join(activeUser) // user, (platfrom, 1)
        .map(x => x._2) //platfrom, 1
        .reduceByKey((a, b) => a + b) //platfrom, activenumber

      val platResult = sc.textFile(args(i + 9))
        .map(_.split("\t"))
        .filter(_.length == 3) //platform, number, ratio
        .map(x => (x(0), x(1)))
        .join(dayPlat) // platform, (number, activenumber)
        .map({case (platfrom, (number, active_number)) =>
          platfrom + "\t" + active_number + "\t" + "%.2f".format(active_number.toDouble / number.toDouble * 100) + "%"})
      platResult.saveAsTextFile(args(i + 12))


      val dayGender = day0.map(x => (x._1, x._2)) // user, gender
        .join(activeUser) //user, (gender, 1)
        .map(x => x._2) //gener, 1
        .reduceByKey((a, b) => a + b) //gender, activenumber

      val genderResult = sc.textFile(args(i + 15))
        .map(_.split("\t"))
        .filter(_.length == 3)
        .map(x => (x(0), x(1))) //gender, number
        .join(dayGender) // gender, (number, activenumber)
        .map({case (gender, (number, active_number)) =>
          gender + "\t" + active_number + "\t" + "%.2f".format(active_number.toDouble / number.toDouble * 100) + "%"})
      genderResult.saveAsTextFile(args(i + 18))

      val day_G_P = day0.map(x => (x._1, (x._2, x._3))) //user, (gender, platform)
        .join(activeUser) //user, ((gender, platform), 1)
        .map(x => x._2)
        .reduceByKey((a, b) => a + b)

      val G_P_Result = sc.textFile(args(i + 21))
        .map(_.split("\t"))
        .filter(_.length == 9)
        .map(x => (x(0), x(1))) //gender & platform, number:num
        .map(x => {
        val gp = x._1.split(" & ")
        val gender = gp(0)
        val platform = gp(1)
        val num = x._2.split(":")(1)
        ((gender, platform), num)
      }).join(day_G_P) // (gender, platform), (number, activenumber)
        .map({case ((gender, plat), (num, ac_num)) =>
          gender + " & " + plat + "\t" + ac_num + "\t" + "%.2f".format(ac_num.toDouble / num.toDouble * 100) + "%"})
      G_P_Result.saveAsTextFile(args(i + 24))

    }


  }
}
