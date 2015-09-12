package com.feel.recommend

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.GaussianMixture

/**
 * Created by canoe on 9/4/15.
 */
object UserCluster {

  private var CLUSTER_NUMBER = 3
  private var ITERATION_NUMBER = 10

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0))
      .map(x => {
      val feature = x.split("\t").tail.map(_.split(":"))
      Vectors.sparse(53, feature.map(_(0).toInt), feature.map(_(1).toDouble))
    })

    CLUSTER_NUMBER = args(3).toInt
    ITERATION_NUMBER = args(4).toInt

    /*val model = KMeans.train(data, CLUSTER_NUMBER, ITERATION_NUMBER)

    val WSSSE = model.computeCost(data)
    println("WSSSE: " + WSSSE)

    val result = sc.textFile(args(0))
      .map(x => {
      val tmp = x.split("\t")
      val id = tmp.head
      val feature = tmp.tail.map(_.split(":"))
      val featureVector = Vectors.sparse(53, feature.map(_(0).toInt), feature.map(_(1).toDouble))
      (model.predict(featureVector), id)
    })
      .sortBy(_._1)

    result.saveAsTextFile(args(1))*/

    val gmm = new GaussianMixture().setK(53).setMaxIterations(ITERATION_NUMBER).run(data)
    gmm.predict(data).saveAsTextFile(args(2))

  }
}