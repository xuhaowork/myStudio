package com.self.core.testBaseApp


import com.self.core.baseApp.myAPP
import com.self.core.syntax.learningPrivate
import scala.collection.mutable.ArrayBuffer



object testMyApp extends myAPP with Serializable {
  override def run(): Unit = {
//    // Test compile.
//    println("Hello world!")
//
//    // Test SparkContext.
//    val rdd = sc.parallelize((1 to 100).toList)
//      .map(x => (x.toDouble, x.toDouble, x.toString))
//
//    // Change the rdd via the class with private param
////    val param = new learningPrivate()
////      .set_height(150.0)
////      .set_weight(171.5)
////      .set_name("DataShoe")
////
////    param.changRdd(rdd)
//
//    // Change the rdd via the companion object
//    val newRdd = learningPrivate.train("DataShoe", 171.5, 155.5, rdd)
//    newRdd.foreach(println)
//
//    val u = Array(1.0, 2.0, 3.0, 4.0, 5.0)

    val arr1 = Array.range(0, 5)
    val arr2 = Array.range(0, 10, 2)

    val u = Array.concat(arr1, arr2)
    u.foreach(println)


    println("-"*80)
    Array.iterate(0, 5)(x => x + 3).foreach(println)

    println("-"*80)
    val newArr = Array.empty[Int] ++ {for(i <- 0 until 5) yield 3*i}
    val newArr2 = ArrayBuffer.empty[Int] ++ {for(i <- 0 until 5) yield 3*i}
    newArr2.foreach(println)

    println("-"*80)
    Array.tabulate(10)(i => Array.fill(i + 1)(0.0)).foreach(x => println(x.length))


    println("-"*80)
    val multiTable = Array.tabulate(9, 9)((i, j) => f"${j + 1}%2d * ${i + 1}%2d = ${(i + 1)*(j + 1)}%2d")
    multiTable.foreach(arr => println(arr.mkString("  ")))

    val triMultiTable = Array.tabulate(9)(i => Array.range(1, i + 2).map(j => f"$j%2d * ${i + 1}%2d = ${(i + 1) * j}%2d"))
    triMultiTable.foreach(arr => println(arr.mkString("  ")))

    println("-"*80)
    triMultiTable.foreach(x => println(x.length))

  }

}
