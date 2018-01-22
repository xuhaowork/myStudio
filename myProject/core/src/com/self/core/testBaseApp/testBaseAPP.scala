package com.self.core.testBaseApp

import com.self.core.baseApp.myAPP


/**
  * Created by DataShoe on 2018/1/5.
  */
object testBaseAPP extends myAPP{
  override def run(): Unit = {
    // Test compile.
    println("Hello World")

    // Test SparkContext.
    val rdd = sc.parallelize((0 until 100).toList)
    val rdd_sum = rdd.reduce(_ + _)
    println(rdd_sum)

  }

}
