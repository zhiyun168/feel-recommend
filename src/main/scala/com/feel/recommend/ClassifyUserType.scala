package com.feel.recommend

import com.feel.utils.{FeelDataRDD, FeelUserRDD}
import org.apache.spark.SparkContext

/**
  * Created by canoe on 2/3/16.
  */
object ClassifyUserType {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val userCard = new FeelUserRDD(sc.textFile(args(0)), List("uid", "cardId"), 0)
      .transform()
      .map(x => (x(1), x(0)))
    val cardTag = new FeelDataRDD(sc.textFile(args(1)), List("cardId", "tag"))
      .transform()
      .map(x => (x(0), x(1)))
    val tagType = new FeelDataRDD(sc.textFile(args(2)), List("tagId", "tagName", "tagType"))
      .transform()
      .map(x => (x(0), x(2)))

    userCard.join(cardTag)
      .map({case (card, (user, tag)) => {
        (tag, user)
      }})
      .join(tagType)
      .map({case (tag, (user, tagType)) => {
        ((user, tagType), 1)
      }}).reduceByKey((a, b) => a + b)
      .map(x => (x._1._1, (x._1._2, x._2)))
      .groupByKey()
      .map(x => {
        val typeAcc = x._2.foldLeft(0, 0)((acc, value) => {
          if (value._1 == "0") {
            (acc._1 + value._2, acc._2)
          } else if (value._1 == "1") {
            (acc._1, acc._2 + value._2)
          } else {
            acc
          }
        })
        x._1 + "\t" + (if (typeAcc._1 > typeAcc._2) 0 else 1).toString
      }).saveAsTextFile(args(3))
  }
}
