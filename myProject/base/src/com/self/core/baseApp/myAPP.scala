package com.self.core.baseApp

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by DataShoe on 2018/1/5.
  */
abstract class myAPP {
  lazy val conf = new SparkConf().setMaster("local").setAppName("DataShoe-Learning")
  lazy val sc = new SparkContext(conf)
  lazy val sqlc = new SQLContext(sc)
  lazy  val smc : StreamingContext = new StreamingContext(conf, Seconds(1))

  def run(): Unit
  def main(args: Array[String]): Unit = {
    run()
  }
}
