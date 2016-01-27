package com.feel.issues

import java.text.SimpleDateFormat
import java.util.Date

import com.feel.utils.FeelUserRDD
import org.apache.spark.SparkContext

/**
  * Created by canoe on 1/26/16.
  */
object TagInfo {

  private val dateBegin = List ("03-21", "04-20", "05-21", "06-22", "07-23",
    "08-23", "09-23", "10-24", "11-23", "12-22", "01-20", "02-19")
  private val dateEnd = List ("04-19", "05-20", "06-21", "07-22", "08-22",
    "09-22", "10-23", "11-22", "12-21", "13-19", "02-18", "03-20")
  private val starBin = List("白羊座", "金牛座", "双子座", "巨蟹座", "狮子座", "处女座",
    "天秤座", "天蝎座", "射手座", "摩羯座", "水瓶座", "双鱼座")

  def ageToBin(age: Int): String = {
      if (age < 16) {
        "0-16"
      } else if (age >= 16 && age <= 20) {
        "16-20"
      } else if (age >= 21 && age <= 25) {
        "21-25"
      } else if (age >= 26 && age <= 30) {
        "26-30"
      } else {
        "30-"
      }
  }

  def birthdayToBin(birthday: String) = {
    if (birthday.size <= 5) {
      "?"
    } else {
      val birthDate = birthday.substring(5)
      var result = ""
      for (i <- 0 until 12) {
        if (birthDate >= dateBegin(i) && birthDate <= dateEnd(i)) {
          result = starBin(i)
        }
      }
      result
    }
  }

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val userRDD = new FeelUserRDD(sc.textFile(args(0)), List("user", "gender", "birthday"), 0)
      .transform()
      .map(x => (x(0), (x(1), x(2))))

    val userBrand = new FeelUserRDD(sc.textFile(args(1)), List("user", "brand"), 0)
      .transform()
      .map(x => (x(0), x(1)))

    userBrand.join(userRDD)
      .flatMap({case(user, (brand, info)) =>
        val gender = info._1
        val birthday = info._2
        val pattern = "([0-9]+)-([0-9][0-9])-([0-9][0-9])".r
        val age = birthday match {
          case pattern(birthdayYear, birthdayMonth, birthdayDay) => {
            val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            val yearDiff = today.substring(0, 4).toInt - birthdayYear.toInt
            val delta = {
              if (today.substring(5, 10) > birthdayMonth + "-" + birthdayDay) 1
              else 0
            }
            if (yearDiff > 100 || yearDiff < 3) 0 else yearDiff + delta
          }
          case _ => 0
        }
        List(((ageToBin(age), brand), 1), ((gender, brand), 1), ((birthdayToBin(birthday), brand), 1))
      }).reduceByKey((a, b) => a + b)
      .map(x => (x._1._1, (x._1._2, x._2)))
      .reduceByKey((a, b) => if (a._2 > b._2) a else b)
      .saveAsTextFile(args(2))
  }
}
