package com.feel.statistics

import com.feel.utils.TimeIssues
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.bson.BSONObject

/**
  * Created by canoe on 2/18/16.
  */


object UserLogInTimeAnalysis {

  def initRDD(sc: SparkContext, authUri: String, inputUri: String): RDD[(Object, BSONObject)] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", authUri)
    hadoopConf.set("mongo.input.uri", inputUri)
    sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])
  }

  def main(args: Array[String]) = {

    val sc = new SparkContext()
    val userLoginLog = initRDD(sc, args(0), args(1))

    val userLoginFeature = userLoginLog.map(x => {
      try {
        val user = x._2.get("uid").toString
        val loginTime = x._2.get("login_time").toString.toLong / 1000
        ((user, TimeIssues.getTsHour(loginTime)), 1)
      } catch {
        case _: Throwable => (("", -1), 0)
      }
    }).filter(_._2 != 0)
      .reduceByKey(_ + _, 100)
      .map(x => (x._1._1, (x._1._2, x._2)))
      .groupByKey()
      .map(x => {
        val user = x._1
        val sum = x._2.map(_._2).sum
        (user, x._2, sum)
      }).filter(_._3 > 3)
      .map(x => {
        val user = x._1
        val sum = x._3
        (user, Vectors.sparse(24, x._2.toSeq.sortWith(_._1 < _._1)
          .map({case (hour, time) => (hour, time / (sum + 0D))})), x._2)
      })

    val kmeans = KMeans.train(userLoginFeature.map(_._2), args(2).toInt, args(3).toInt)
    val dataModel = userLoginFeature.map(x => {
      (x._1, x._2, kmeans.predict(x._2), x._3)
    })
    dataModel.map(x => (x._1, x._3)).saveAsTextFile(args(4))
    dataModel.map(x => (x._3, 1)).reduceByKey(_ + _).saveAsTextFile(args(5))
    dataModel.flatMap(x => x._4.map({case (index, value) => ((x._3, index), value)}))
      .groupByKey()
      .map(x => {
        val mean = x._2.foldLeft(0D)((acc, value) => acc + value) / x._2.size
        (x._1._1, (x._1._2, mean))
      }).groupByKey()
      .map(x => x._1 + "\t" + x._2.map(y => y._1.toString + "," + y._2.toString).mkString("\t"))
      .saveAsTextFile(args(6))
    dataModel.saveAsTextFile(args(7))
  }
}
