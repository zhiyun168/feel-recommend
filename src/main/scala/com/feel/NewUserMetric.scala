package com.feel.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by canoe on 8/20/15.
 */
object NewUserMetric {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val newUserRDD = sc.textFile(args(0))
      .map(_.split(("\t")))
      .filter(_.length == 3)

    def age(birthday: String) = {
      val pattern = "([0-9]+)-([0-9][0-9])-([0-9][0-9])".r
      birthday match {
        case pattern(birthdayYear, birthdayMonth, birthdayDay) => {
          val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
          val yearDiff = today.substring(0, 4).toInt - birthdayYear.toInt
          val delta = {
            if (today.substring(5, 10) > birthdayMonth + "-" + birthdayDay) 1
            else -1
          }
          yearDiff + delta
        }
        case _: String => -1
      }
    }

    val newUserAge = newUserRDD
      .map(x => ((x(1), age(x(2))), 1))
      .reduceByKey((a, b) => a + b)
      .map(x => x._1._1 + "\t" + x._1._2 + "\t" + x._2)
    newUserAge.saveAsTextFile(args(5))

    val newUserGender = newUserRDD.map(x => (x(0), x(1)))

    val tagName = sc.textFile(args(3))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))

    val newUserFollowedTag = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1))) //user, tag
      .join(newUserGender)
      .map(x => ((x._2._2, x._2._1), 1)) //（gender, tag), number
      .reduceByKey((a, b) => a + b)
      .map(x => (x._1._2, (x._1._1, x._2))) // tag, gender, number
      .join(tagName)
      .map(x => x._1 + "\t" + x._2._2 + "\t" + x._2._1._1 + "\t" + x._2._1._2) //tag, tagName, gender, number

    newUserFollowedTag.saveAsTextFile(args(6))

    val goalName = sc.textFile(args(4))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))

    val newUserJoinedGoal = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1))) // user, goal
      .join(newUserGender)
      .map(x => ((x._2._2, x._2._1), 1)) //(gender, goal), number
      .reduceByKey((a, b) => a + b)
      .map(x => (x._1._2, (x._1._1, x._2))) // goal, gender, number
      .join(goalName)
      .map(x => x._1 + "\t" + x._2._2 + "\t" + x._2._1._1 + "\t" + x._2._1._2) // goal, goalName, gender, number
    newUserJoinedGoal.saveAsTextFile(args(7))

  }
}
