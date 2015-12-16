package com.feel.recommend

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.types.BasicBSONList
import org.bson.BSONObject

/**
  * Created by canoe on 12/7/15.
  */

object RecommendPlanForStepTarget {

  private val LEAST_STEP_RATIO = 0.05

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))

    val stepRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userStepAverageNumber = stepRDD.map(x => {
      val user = x._2.get("uid").toString
      val goalType = x._2.get("goal_type").toString
      val stepInfo = {
        val step = try {
          goalType match {
            case "3" => {
              val deviceType = x._2.get("device")
              val stepScore = deviceType match {
                case "health" =>
                  x._2.get("info").asInstanceOf[BSONObject].get("records").asInstanceOf[BasicBSONList].toArray()
                  .map(_.asInstanceOf[BSONObject].get("step").toString.toInt).zipWithIndex.toList
              }
              stepScore
            }
            case _ => Nil
          }
        } catch {
          case _ => Nil
        }
        step
      }
      (user, stepInfo)
    }).filter(_._2 != Nil)
    .flatMap(x => {
      x._2.map(y => ((x._1, y._2), y._1))
    }).groupByKey()
    .map(x => {
      val user = x._1._1
      val hour = x._1._2
      val mean = x._2.foldLeft(0D)((acc, value) => acc + value) / x._2.size
      (user, (hour, mean))
    }).groupByKey()
    .map(x => {
      val stepSum = x._2.map(_._2).sum
      (x._1, {
        val hourRatio = x._2.toList.sortWith(_._1 < _._1).map(x => (x._1, x._2 / stepSum))
          .filter(_._2 > LEAST_STEP_RATIO)
        val rationInfo = hourRatio.map(_._2).sum
        val target = hourRatio.map(x => (x._1, x._2 / rationInfo))
        target
      })
    })

    userStepAverageNumber.saveAsTextFile(args(2))
  }
}
