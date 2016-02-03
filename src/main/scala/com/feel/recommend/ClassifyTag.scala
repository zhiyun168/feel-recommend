package com.feel.recommend

import com.feel.utils.FeelDataRDD
import org.apache.spark.SparkContext

/**
  * Created by canoe on 2/2/16.
  */
object ClassifyTag {

  private var CO_EXIST_TAG = 70
  private val FASHION_JUDGE_MAP = Map("1144" -> 0, "1152" -> 0, "143" -> 0,
    "14774" -> 0, "24595" -> 0, "126" -> 0,
    "533" -> 0, "175" -> 0, "885" -> 0,
    "5985" -> 1, "1041" -> 1, "2395" -> 1)

  def main(args: Array[String]) = {
    val sc = new SparkContext()
    val cardTag = new FeelDataRDD(sc.textFile(args(0)), List("card", "tagId", "cardName")).transform()

    CO_EXIST_TAG = args(2).toInt
    cardTag.map(x => (x(0), (x(1), x(2))))
      .groupByKey()
      .flatMap(x => {
        val tagList = x._2.toList
        val tagSize = tagList.size
        val result = new Array[(((String, String), (String, String)), Int)](tagSize * tagSize)
          for (i <- 0 until tagSize) {
            for (j <- 0 until i) {
              if (i != j) {
                result(i * tagSize + j) = ((tagList(i), tagList(j)), 1)
              }
            }
          }
        result.filter(_ != null).toSeq
      })
      .reduceByKey((a, b) => a + b)
      .filter(_._2 > CO_EXIST_TAG)
      .map(x => {
        (x._1._1, (x._1._2, x._2))
      })
      .groupByKey()
      .map(x => {
        val tag = x._1
        if (FASHION_JUDGE_MAP.contains(tag._1)) {
          tag._1.toString + "\t" + tag._2.toString + "\t" + (FASHION_JUDGE_MAP.getOrElse(tag._1, -1)).toString
        } else {
          val typeValue = x._2.foldLeft((0, 0))((acc, value) => {
            val tagType = FASHION_JUDGE_MAP.getOrElse(value._1._1, -1)
            if (tagType == 0) {
              (acc._1 + value._2, acc._2)
            } else {
              (acc._1, acc._2 + value._2)
            }
          })
          tag._1.toString + "\t" + tag._2.toString + "\t" + (if (typeValue._1 > typeValue._2) 0 else 1).toString
        }
      })
      .saveAsTextFile(args(1))
  }
}
