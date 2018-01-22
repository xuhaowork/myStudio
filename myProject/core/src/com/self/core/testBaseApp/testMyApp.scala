package com.self.core.testBaseApp


import com.self.core.baseApp.myAPP
import com.self.core.syntax.learningPrivate



object testMyApp extends myAPP with Serializable {
  override def run(): Unit = {
    // Test compile.
    println("Hello world!")

    // Test SparkContext.
    val rdd = sc.parallelize((1 to 100).toList)
      .map(x => (x.toDouble, x.toDouble, x.toString))

    // Change the rdd via the class with private param
//    val param = new learningPrivate()
//      .set_height(150.0)
//      .set_weight(171.5)
//      .set_name("DataShoe")
//
//    param.changRdd(rdd)

    // Change the rdd via the companion object
    val newRdd = learningPrivate.train("DataShoe", 171.5, 155.5, rdd)
    newRdd.foreach(println)

    val u = Array(1.0, 2.0, 3.0, 4.0, 5.0)
    sc.broadcast(u)



  }

}
