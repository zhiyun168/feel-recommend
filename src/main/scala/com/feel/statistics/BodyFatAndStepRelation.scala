package com.feel.statistics

import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BSONObject

/**
  * Created by canoe on 12/8/15.
  */
object BodyFatAndStepRelation {

  private val FAT_VALUE_CONFIG = List("viseral_fat_level", "muscle_rate", "bmr",
    "weight", "water_rate", "bmi", "protein_race", "body_score", "body_fat_race", "bone_mass")

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

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
    val bodyRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
    classOf[BSONObject])

    val userBodyInfo = bodyRDD.filter(x => x._2.get("device").toString.equalsIgnoreCase("picooc"))
    .flatMap(x => {
      val user = x._2.get("uid").toString
      val bodyInfo = x._2.get("info").asInstanceOf[BSONObject]
      FAT_VALUE_CONFIG.map(x => ((user, x), try {
        bodyInfo.get(x).toString.toDouble
      } catch {
        case _ => Double.MinValue
      }))
    }).groupByKey()
    .map(x => (x._1._1, (x._1._2, {
      x._2.foldLeft(0D)((acc, value) => {
        acc + value
      }) / x._2.size
    })))
      .filter(_._2._2 > 0D)
      .groupByKey()
      .map(x => {
        (x._1, x._2.toArray.sortWith(_._1 < _._1))
      })

    val userStepAndBodyInfo = userStepAverageNumber.join(userBodyInfo)

    userStepAndBodyInfo.saveAsTextFile(args(4))

    val data = userStepAndBodyInfo.map(x => Vectors.dense(x._2._2.map(_._2)))
    val corrInfoMatrix = Statistics.corr(data)
    val matrixElements = for (i <- 0 until 100)
      yield(FAT_VALUE_CONFIG(i / 10), FAT_VALUE_CONFIG(i % 10), corrInfoMatrix(i / 10, i % 10))

    val corrInfo = sc.parallelize(matrixElements.toList)

    corrInfo.saveAsTextFile(args(5))
  }
}
