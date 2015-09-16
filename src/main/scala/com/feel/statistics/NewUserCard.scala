package com.feel.statistics

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

/**
 * Created by aidi.feng on 15/9/10.
 */
object NewUserCard {
  def main (args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext()

    val android = "[a-zA-Z0-9_]+android[_a-zA-z0-9]*"
    val ios = "[a-zA-Z0-9_]+ios"

    val dataRDD = sc.textFile(args(0))
      .map(_.replaceAll(android, "android"))
      .map(_.replaceAll(ios, "ios"))
      .map(_.split("\t"))
      .filter(_.length == 5)
      .map(x => (x(0), x(1), x(2), x(3), x(4))) //user, gender, platform, type, status^is_del
      .map(x => {
      val p = if (x._3 != "android" && x._3 != "ios") "unknown" else x._3
      (x._1, x._2, p, x._4, x._5)
    })


    val userRDD = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(1), 1)) //gender
    val totalUser = userRDD.count()
    val GenderNum = userRDD.reduceByKey((a, b) => a + b)

    val GenderPlatNum = sc.textFile(args(1))
      .map(_.replaceAll(android, "android"))
      .map(_.replaceAll(ios, "ios"))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(1), x(0))) //gender, platform
      .map(x => {
      val p  = if (x._2 != "android" && x._2 != "ios") "unknown" else x._2
      ((x._1, p), 1)
    })
      .reduceByKey((a, b) => a + b) //platform, number


    val totalCard = dataRDD.count()
    val CardUser = dataRDD.map(x => x._1).distinct().count()

    val genderRDD = dataRDD.map(x => (x._2, (x._1, x._3, x._4))) //gender, user, platform, type
    // val genderRDD = dataRDD.map({case (a, b, c, d) => (a, (b, c, d))})
     /*val genderRDD = dataRDD.map( x => x match {
      case (a, b, c, d) => (a, (b, c, d))
      case _ =>
    })*/

    val genderDCard = genderRDD.map(x => (x._1, x._2._1))//the card number of distinct user group by gender*/
      .groupByKey()
      .map(x => (x._1, x._2.toSet.size))

    val genderTCard = genderRDD.map(x => (x._1, 1))
      .reduceByKey((a, b) => a + b) //the card number of group by gender
      .join(genderDCard).join(GenderNum) //gender, ((cardnumber, distinctuser), gendernum)
      .map({case (gender, ((cardnum, distuser), number)) => "gender:" + gender +
      "\tnumber:" + number + "\t" + (number.toDouble / totalUser) +
      "\tpostCardNumber:" + distuser + "\t" + (distuser.toDouble / number) +
      "\tcardNumer:" + cardnum + "\taverageCard:" + (cardnum.toDouble / distuser)})
    // gender number distinct_user distinct_user/number card_number card_number/distinct_user
    genderTCard.saveAsTextFile(args(2))


    val RegisterPlatform = sc.textFile(args(1))
      .map(_.replaceAll(android, "android"))
      .map(_.replaceAll(ios, "ios"))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), 1)) //platform
      .map(x => {
        val p  = if (x._1 != "android" && x._1 != "ios") "unknown" else x._1
        (p, x._2)
    })
      .reduceByKey((a, b) => a + b) //platform, number

    val platformCard = dataRDD.map(x => (x._3, x._1))
      .groupByKey() //(platform, user)
      .map(x => (x._1, x._2.toSet.size)) //platform, number
      .join(RegisterPlatform) //platform, (postcardnumber, number)
      .map(x => x._1 + "\tnumber:" + x._2._2 + "\tpostCardNumber:" + x._2._1 + "\t" + (x._2._1.toDouble / x._2._2))

    platformCard.saveAsTextFile(args(3))

    //the card situation of different platfrom and gender
    val postCard = dataRDD.map(x => ((x._2, x._3), (x._4, x._5))) //(gender, platform), (type, status^is_del)
      .groupByKey()
      .join(GenderPlatNum) //(gender, platform), ({(type, status^is_del)}, num)
      .map({case((gender, plat), st) => {
      val mp = new mutable.HashMap[String, Int]()
      for (card <- st._1) {
        if (card._1 == "card") {
          if (mp.get("card").isEmpty) mp("card") = 1
          else mp("card") += 1
        }
        if (card._1 == "goal") {
          if (mp.get("goal").isEmpty) mp("goal") = 1
          else mp("goal") += 1

          if (card._2 == "0") {
            if (mp.get("noContent").isEmpty) mp("noContent") = 1
            else mp("noContent") += 1
          } else if (card._2 == "2") {
            if (mp.get("hasContent").isEmpty) mp("hasContent") = 1
            else mp("hasContent") += 1
          }
        }
      }
      val sz = st._1.size
      val picture = if (mp.get("card").isEmpty) 0 else mp("card")
      val goal = if (mp.get("goal").isEmpty) 0 else mp("goal")
      val hasC = if (mp.get("hasContent").isEmpty) 0 else mp("hasContent")
      val noC = if (mp.get("noContent").isEmpty) 0 else mp("noContent")
      gender + " & " + plat+ "\tnumber:" + st._2 + "\tcardNumber:" + sz + "\tpicture:" + picture + "\t" + (picture.toDouble / sz) +
        "\tgoal:" + goal + "\t" + (goal.toDouble / sz) +
        "\thasContent:" + hasC + "\tnoContent:" + noC
    }})

    postCard.saveAsTextFile(args(4))

  }
}
