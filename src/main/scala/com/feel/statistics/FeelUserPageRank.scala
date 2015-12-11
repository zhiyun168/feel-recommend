package com.feel.statistics

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}

/**
  * Created by canoe on 12/11/15.
  */
object FeelUserPageRank {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val followEdge = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => new Edge(x(1).toLong, x(0).toLong, 1L))

    val graph = Graph.fromEdges(followEdge, None)
    val rank = graph.pageRank(0.0001, 0.15)
    rank.vertices.saveAsTextFile(args(1))
  }
}

