package com.self.core.testBaseApp

import com.self.core.baseApp.myAPP

object testBaseApp extends myAPP{
  override def run(): Unit = {
    // Test compile.
    println("Hello world!")

    // Test SparkContext.
    val rdd = sc.parallelize((1 to 100).toList)
    val rdd_sum = rdd.reduce(_ + _)
    println(rdd_sum)


  }

}
