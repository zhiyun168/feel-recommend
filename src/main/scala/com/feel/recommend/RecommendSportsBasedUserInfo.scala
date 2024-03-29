package com.feel.recommend

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject
import org.elasticsearch.spark._

case class SportsRecommend(user: String, candidates: Seq[String])
/**
  * Created by canoe on 12/14/15.
  */
object RecommendSportsBasedUserInfo {
  //Feel Old & Yellow Calendar

  private val sportsSetList = List(
    // 000, m, notFat, goodSleep
    Set("打羽毛球", "踢足球", "打篮球", "打网球", "攀岩", "爬山", "跑步", "跳舞", "滑冰", "滑雪", "骑行", "健身"),
    // 001, f, notFat, goodSleep
    Set("打羽毛球", "跑步", "练瑜伽", "打网球", "攀岩", "爬山", "滑冰", "跳舞", "骑行", "健身"),
    // 010, m, fat, goodSleep
    Set("打羽毛球", "慢走", "骑行", "慢跑"),
    // 011, f, fat, goodSleep
    Set("打羽毛球", "慢走", "瑜伽", "慢走", "慢跑", "骑行"),
    // 100, m, notFat, badSleep
    Set("滑冰", "健身"),
    // 101, f, notFat, badSleep
    Set("滑冰", "练习健美操", "练瑜伽", "跳舞", "滑雪", "健身"),
    // 110, m, fat, badSleep
    Set("健身"),
    // 111, f, fat, badSleep
    Set("瑜伽", "健身"))

  private val ALL_SET = Set("打乒乓球", "打高尔夫", "踢毽子", "打台球", "练习太极", "慢走", "做广播体操")

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(4))
    val sc = new SparkContext(conf)

    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))

    val fatRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userFatInfo = fatRDD.filter(x => {
      try {
        x._2.get("device").toString.equalsIgnoreCase("picooc")
      } catch {
        case _: Throwable => false
      }})
      .map(x => {
        val user = x._2.get("uid").toString
        val fatInfo = try {
          val bodyInfo = x._2.get("info").asInstanceOf[BSONObject]
          val fatInfo =  bodyInfo.get("body_fat_race").toString.toDouble
          val ts = bodyInfo.get("created").toString.toLong
          (fatInfo, ts)
        } catch {
          case _: Throwable => (Double.MinValue, -1L)
        }
        (user, fatInfo)
      }).filter(_._2._1 != Double.MinValue)
      .reduceByKey((a, b) => if (a._2 < b._2) b else a)
      .map(x => {
        (x._1, if (x._2._1 > 30) "fat" else "notFat")
      })

    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))

    val sleepRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userSleepInfo = sleepRDD.filter(x => try {
      x._2.get("device").toString.equalsIgnoreCase("mi_band")
    } catch {
      case _: Throwable => false
    })
    .map(x => {
      val user = x._2.get("uid").toString
      val sleepInfo = try {
        val miBandInfo = x._2.get("info").asInstanceOf[BSONObject]
        val shallowSleepTime = miBandInfo.get("shallowSleepTime").toString.toDouble
        val deepSleepTime = miBandInfo.get("deepSleepTime").toString.toDouble
        val ts = miBandInfo.get("created").toString.toLong
        (shallowSleepTime, deepSleepTime, ts)
      } catch {
        case _: Throwable => (-1D, -1D, -1L)
      }
      (user, sleepInfo)
    }).filter(_._1 != -1)
      .reduceByKey((a, b) => if (a._3 < b._3) b else a)
      .map(x => {
        (x._1, if (x._2._1 + x._2._2 > 360 && x._2._2 > 180) "goodSleep" else "badSleep")
    })

    val userGender = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))

    val userSports = userGender.union(userFatInfo).union(userSleepInfo)
      .groupByKey()
      .map(x => {
        val user = x._1
        val infoSet = x._2.toSet
        var mask = 0
        if (infoSet.contains("f")) {
            mask |= 1
        }
        if (infoSet.contains("fat")) {
          mask |= 2
        }
        if (infoSet.contains("badSleep")) {
          mask |= 4
        }
        (user, (mask, (sportsSetList(mask) | ALL_SET).toSeq))
      })
    userSports.saveAsTextFile(args(3))
    userSports.map(x => SportsRecommend(x._1, x._2._2)).saveToEs("recommend/sports")
  }
}
