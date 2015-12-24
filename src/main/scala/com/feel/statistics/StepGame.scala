package com.feel.statistics

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.bson.BSONObject
import org.bson.types.BasicBSONList

/**
  * Created by canoe on 12/23/15.
  */
object StepGame {

  def main(args: Array[String]) = {
    val sc = new SparkContext()
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val mongoRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userStepNumber = mongoRDD
    .filter(_._2.get("device").toString.equalsIgnoreCase("pedometer"))
    .map(x => {
      val user = x._2.get("uid").toString
      val stepList = try {
        x._2.get("info").asInstanceOf[BSONObject].get("steps").asInstanceOf[BasicBSONList].toArray()
          .map(_.toString.toDouble).zipWithIndex.toList
      } catch {
        case _ => Nil
      }
      (user, stepList)
    }).filter(_._2 != Nil)
     .flatMap(x => {
       x._2.map(y => ((x._1, y._2), y._1))
     }).groupByKey()
      .map(x => {
        (x._1._1, (x._1._2, x._2.foldLeft(0D)((acc, value) => acc + value) / x._2.size))
      })
      .groupByKey()
      .filter(_._2.map(_._2).max > 500D)
      .map(x => {
        val maxValue = x._2.map(_._2).max
        (x._1, Vectors.dense(x._2.toArray.sortWith(_._1 < _._1).map(_._2 / maxValue)))
      })

    args(5) match {
      case "kmeans" => {
        val model = KMeans.train(userStepNumber.map(_._2), args(3).toInt, args(4).toInt)
        userStepNumber.flatMap(x => {
          val cluster = model.predict(x._2)
          x._2.toArray.zipWithIndex.map(y => ((cluster, y._2), y._1))
        }).groupByKey()
        .map(x => {
          val cluster = x._1._1
          val hour = x._1._2
          val hourStepMean = x._2.foldLeft(0D)((acc, value) => acc + value) / x._2.size
          (cluster, (hour, hourStepMean))
        }).groupByKey()
        .map(x => {
          x._1 + "\t" + x._2.toSeq.sortWith(_._1 < _._1).map(_._2).mkString(";")
        }).saveAsTextFile(args(2))

      }
      case "gmm" => {
        val model = new GaussianMixture().setK(args(3).toInt).setMaxIterations(args(4).toInt)
          .run(userStepNumber.map(_._2))
        userStepNumber.flatMap(x => {
          val cluster = model.predict(x._2)
          x._2.toArray.zipWithIndex.map(y => ((cluster, y._2), y._1))
        }).groupByKey()
          .map(x => {
            val cluster = x._1._1
            val hour = x._1._2
            val hourStepMean = x._2.foldLeft(0D)((acc, value) => acc + value) / x._2.size
            (cluster, (hour, hourStepMean))
          }).groupByKey()
          .map(x => {
            x._1 + "\t" + x._2.toSeq.sortWith(_._1 < _._1).map(_._2).mkString(";")
          }).saveAsTextFile(args(2))
      }
    }
  }
}
