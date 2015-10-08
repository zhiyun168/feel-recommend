package com.feel.statistics

import org.apache.spark.{SparkConf, SparkContext}

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
      .map(_.replaceAll("x", "f"))
      .map(_.split("\t"))
      .filter(_.length == 5)
      .map(x => (x(0), x(1), x(2), x(3), x(4))) //user, gender, platform, type, status^is_del
      .map(x => {
      val p = if (x._3 != "android" && x._3 != "ios") "ios" else x._3
      (x._1, x._2, p, x._4, x._5)
    })


    val userRDD = sc.textFile(args(1))
      .map(_.replaceAll("x", "f"))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(1), 1)) //gender
    val totalUser = userRDD.count()
    val GenderNum = userRDD.reduceByKey((a, b) => a + b)

    val GenderPlatNum = sc.textFile(args(1))
      .map(_.replaceAll(android, "android"))
      .map(_.replaceAll(ios, "ios"))
      .map(_.replaceAll("x", "f"))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(1), x(0))) //gender, platform
      .map(x => {
      val p  = if (x._2 != "android" && x._2 != "ios") "ios" else x._2
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

    val distinctUser = genderRDD.map(x => (x._1, x._2._1))//the card number of distinct user group by gender*/
      .groupByKey()
      .map(x => (x._1, x._2.toSet.size))

    val genderResult = genderRDD.map(x => (x._1, 1))
      .reduceByKey((a, b) => a + b) //the card number of group by gender
      .join(distinctUser).join(GenderNum) //gender, ((cardnumber, distinctuser), gendernum)
      .map({case (gender, ((cardnum, distuser), number)) => "性别:" + gender +
      "\t注册数:" + number + "\t占比:" + "%.2f".format(number.toDouble / totalUser * 100) + "%" +
      "\t发动态人数:" + distuser + "\t占比:" + "%.2f".format(distuser.toDouble / number * 100) + "%" +
      "\t动态数:" + cardnum + "\t人均动态数:" + "%.2f".format(cardnum.toDouble / distuser)})
    // gender number distinct_user distinct_user/number card_number card_number/distinct_user
    genderResult.saveAsTextFile(args(2))


    val RegisterPlatform = sc.textFile(args(1))
      .map(_.replaceAll(android, "android"))
      .map(_.replaceAll(ios, "ios"))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), 1)) //platform
      .map(x => {
        val p  = if (x._1 != "android" && x._1 != "ios") "ios" else x._1
        (p, x._2)
    })
      .reduceByKey((a, b) => a + b) //platform, number

    val platformCard = dataRDD.map(x => (x._3, x._1))
      .groupByKey() //(platform, user))
      .map(x => (x._1, x._2.toSet.size)) //platform, number
      .join(RegisterPlatform) //platform, (postcardnumber, number)
      .map(x => x._1 + "\t注册数:" + x._2._2 + "\t发动态人数:" + x._2._1 + "\t占比:" + "%.2f".format(x._2._1.toDouble / x._2._2 * 100) + "%")

    platformCard.saveAsTextFile(args(3))

    //the card situation of different platfrom and gender
    val postCard = dataRDD.map(x => ((x._2, x._3), (x._4, x._5))) //(gender, platform), (type, status^is_del)
      .groupByKey()
      .join(GenderPlatNum) //(gender, platform), ({(type, status^is_del)}, num)
      .map({case((gender, plat), st) => {

      //(picture, goal, hascontent, nocontent
      val ans = st._1.foldLeft(0, 0, 0, 0) { (res, card) =>
        card._1 match {
          case "card" => (res._1 + 1, res._2, res._3, res._4)
          case "goal" => {
            card._2 match {
              case "0" => (res._1, res._2 + 1, res._3, res._4 + 1)
              case "2" => (res._1, res._2 + 1, res._3 + 1, res._4)
              case _ => res
            }
          }
          case _ => res
      }
    }
      val sz = st._1.size
      val picture = ans._1
      val goal = ans._2
      val hasC  = ans._3
      val noC = ans._4
      gender + " & " + plat+ "\t注册数:" + st._2 + "\t动态数:" + sz +
        "\t图片:" + picture + "\t占比:" + "%.2f".format(picture.toDouble / sz * 100) + "%" +
        "\t打卡:" + goal + "\t占比:" + "%.2f".format(goal.toDouble / sz * 100) + "%" +
        "\t有内容打卡:" + hasC + "\t无内容打卡:" + noC
    }})

    postCard.saveAsTextFile(args(4))

    val picture = dataRDD.filter(_._4 == "card").count()
    val res = sc.parallelize(List("发动态人数:" + CardUser + "\t占比:" + "%.2f".format(CardUser.toDouble / totalUser * 100) + "%",
    "图片内容:" + picture + "\t占比:" + "%.2f".format(picture.toDouble / totalCard * 100) + "%"))
    res.saveAsTextFile(args(5))

  }
}
