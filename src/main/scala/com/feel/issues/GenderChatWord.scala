package com.feel.issues

import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BSONObject
import org.bson.types.BasicBSONList
import org.apache.hadoop.conf.Configuration

/**
 * Created by canoe on 11/2/15.
 */
object GenderChatWord {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val hadoopConf = new Configuration()

    hadoopConf.set("mongo.auth.uri", args(0))
    hadoopConf.set("mongo.input.uri", args(1))
    val chatData = sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

    val userChat = chatData.map(x => {
      val user = x._2.get("from")
      val msg = x._2.get("payload").asInstanceOf[BSONObject].get("bodies").asInstanceOf[BasicBSONList].get(0)
        .asInstanceOf[BSONObject].get("msg").toString.replaceAll("[\\[\\]\t ,.;?!！，；。]", "")
      (user, msg)
    }).groupByKey()
      .filter(_._2.size <= 100)
      .map(x => (x._1, x._2.mkString("\t")))
    userChat.saveAsTextFile(args(2))

   /* val userGender = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      */

  }
}
