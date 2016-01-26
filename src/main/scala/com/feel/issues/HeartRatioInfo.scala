package com.feel.issues

import com.feel.utils.{TimeIssues, FeelUserHardWareRDD, MongoReader}

/**
  * Created by canoe on 1/26/16.
  */
object HeartRatioInfo {

  def main(args: Array[String]) = {
    val mongoReader = new MongoReader(args(0), args(1))
    val heartRatioRDD = new FeelUserHardWareRDD(mongoReader.initRDD(), "7",
      (TimeIssues.nDaysAgoTs(400), TimeIssues.nDaysAgoTs(0)), "bpm")
      .transform()

    heartRatioRDD.map(x => (x._1, x._2))
      .sortBy(_._2)
      .saveAsTextFile(args(2))
  }
}
