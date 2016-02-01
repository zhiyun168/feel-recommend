package com.feel.statistics

import com.feel.utils.{JsonUtil, TimeIssues}
import org.apache.spark.SparkContext


import scala.collection.mutable


/**
  * Created by canoe on 1/20/16.
  */


object AllReport {

  def parseToList(str: String): List[String] = {
    str.replaceAll("[\\[\\]\\\" ]", "")
      .split("\\,").toList
  }

  def parseToList(str: String, depth: Int): List[List[Long]] = {
    str.replaceAll("[\\[\\]\\\" ]", "")
      .split(",").foldLeft((0, List[Long](), List[List[Long]]())) ((acc, value) => {
      ((acc._1 + 1) % 2,
        if (acc._1 == 1) List() else value.toLong :: acc._2,
        if (acc._1 == 1) (value.toLong :: acc._2).reverse :: acc._3 else acc._3)
    })._3.reverse
  }

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val data = sc.textFile(args(0))
      data.map(_.split("\t"))
          .filter(_.length == 2)
          .map(x => (x(0), x(1)))
          .repartition(100)
          .groupByKey()
          .map(x => {
            val report = new mutable.HashMap[String, Any]
            report.put("updated", System.currentTimeMillis() / 1000)
            report.put("report_date", List(TimeIssues.nDaysAgoDate(8), TimeIssues.nDaysAgoDate(1)))
            report.put("uid", x._1)
            val allReport = x._2.foldLeft(report)((acc, value) => {
              val tmp = value.split(":")
              tmp(0) match {
                case "user_heart_ratio" => {
                  acc.put(tmp(0), parseToList(tmp(1)))
                  acc
                }
                case "info_user_heart_ratio_mean" => {
                  acc.put(tmp(0), parseToList(tmp(1)))
                  acc
                }
                case "user_step_rank_ratio" => {
                  acc.put(tmp(0), parseToList(tmp(1)))
                  acc
                }
                case "user_daily_max_distance" => {
                  acc.put(tmp(0), parseToList(tmp(1)))
                  acc
                }
                case "step_trend" => {
                  acc.put(tmp(0), parseToList(tmp(1), 2))
                  acc
                }
                case _ => {
                  if (tmp(1).contains(".")) {
                    acc.put(tmp(0), tmp(1).toDouble)
                  } else {
                    acc.put(tmp(0), tmp(1).toLong)
                  }
                  acc
                }
              }
            })
            JsonUtil.toJson(allReport)
          }).saveAsTextFile(args(1))
  }
}
