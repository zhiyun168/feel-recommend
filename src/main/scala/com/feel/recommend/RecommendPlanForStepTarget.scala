package com.feel.recommend

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.types.BasicBSONList
import org.bson.BSONObject

/**
  * Created by canoe on 12/7/15.
  */

object RecommendPlanForStepTarget {

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
      val stepInfo = {
        val step = try {
          goalType match {
            case "3" => {
              val deviceType = x._2.get("device")
              val stepScore = deviceType match {
                case "health" =>
                  x._2.get("info").asInstanceOf[BSONObject].get("records").asInstanceOf[BasicBSONList].toArray()
                  .zipWithIndex.map(e => e._1 + ":" + e._2.toString).mkString(",")
              }
              stepScore
            }
            case _ => ""
          }
        } catch {
          case _ => ""
        }
        step
      }
      (user, stepInfo)
    }).filter(_._2 != "")

    userStepAverageNumber.saveAsTextFile(args(2))
  }
}
