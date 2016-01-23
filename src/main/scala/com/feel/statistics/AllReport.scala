package com.feel.statistics

import com.feel.utils.TimeIssues
import org.apache.spark.SparkContext

import org.codehaus.jettison.json.JSONObject


/**
  * Created by canoe on 1/20/16.
  */
object AllReport {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val data = sc.textFile(args(0))
      data.map(_.split("\t"))
          .filter(_.length == 2)
          .map(x => (x(0), x(1)))
          .groupByKey()
          .map(x => {
            val report = new JSONObject()
            report.put("updated", System.currentTimeMillis() / 1000)
            report.put("report_date", TimeIssues.nDaysAgoDate(-8) + ";" + TimeIssues.nDaysAgoDate(-1))
            x._2.foldLeft(report.put("uid", x._1))((acc, value) => {
              val tmp = value.split(":")
              acc.put(tmp(0), tmp(1))
            })
          })
          .saveAsTextFile(args(1))
  }
}
