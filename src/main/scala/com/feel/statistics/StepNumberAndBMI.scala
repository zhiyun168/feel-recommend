package com.feel.statistics

import breeze.stats.distributions.WeibullDistribution
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.bson.BSONObject

/**
  * Created by canoe on 11/27/15.
  */
object StepNumberAndBMI {

  def main(args: Array[String]) = {

    val sc = new SparkContext()

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val stepRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userStepAverageNumber = stepRDD.map(x => {
      val user = x._2.get("uid")
      val goalType = x._2.get("goal_type").toString
      val stepNumber = {
        val step = try {
          goalType match {
            case "3" => {
              val deviceType = x._2.get("device")
              val stepScore = deviceType match {
                case "mi_band" =>
                  x._2.get("info").asInstanceOf[BSONObject].get("step").toString.toLong
                case _ =>
                  x._2.get("info").asInstanceOf[BSONObject].get("sum").asInstanceOf[BSONObject].get("step")
                    .toString.toLong
              }
              stepScore
            }
            case _ => 0
          }
        } catch {
          case _ => 0
        }
        step
      }
      (user, stepNumber)
    }).filter(_._2 >= 100)
    .groupByKey()
    .map(x => {
      val user = x._1.toString
      val average = x._2.foldLeft((0D))((acc, value)  => {
        acc + value
      }) / x._2.size
      (user, (average / 1000).toInt)
    })

    hadoopConf.set("mongo.auth.uri", args(2))
    hadoopConf.set("mongo.input.uri", args(3))

    val bmiRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    def bmi(height: Double, weight: Double) = {
      val h = height / 100D
      weight / (h * h)
    }

    val userBMI = bmiRDD.map(x => {
      try {
        val user = x._2.get("_id").toString.toLong
        val height = x._2.get("height").toString.toDouble
        val weight = x._2.get("weight").toString.toDouble
        (user, height, weight)
      } catch {
        case _ => ("", 0D, 0D)
      }
    })
    .filter(x => (x._2 != 0D && x._3 != 0D))
    .map(x => {
      (x._1.toString, bmi(x._2, x._3).toInt)
    })


    val stepAndBMI = userStepAverageNumber.join(userBMI)
      .map(x => (x._2, 1))
      .reduceByKey((a, b) => a + b)
      .map(x => (x._1._1 + "\t" + x._1._2 + "\t" + x._2.toString))

    stepAndBMI.saveAsTextFile(args(4))

    userStepAverageNumber.map(x => (x._2, 1)).reduceByKey((a, b) => a + b).saveAsTextFile(args(5))
    userBMI.map(x => (x._2, 1)).reduceByKey((a, b) => a + b).saveAsTextFile(args(6))
  }
}
