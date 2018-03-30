package com.self.core.testBaseApp

import com.self.core.baseApp.myAPP
import src.com.self.core.geoHash.{GeoHash, GeoHashMap}


/**
  * Created by DataShoe on 2018/1/5.
  */
object testBaseAPP extends myAPP{
  override def run(): Unit = {
    val rd = new java.util.Random(123L)
    val data = Array.fill(100000)((rd.nextDouble()*10, rd.nextDouble()*50 + 50))
    val geo = new GeoHash(8)
    val geo2 = new GeoHashMap(8)
    geo.encode(0.1, 0.1)
    geo2.encode(0.1, 0.1)

    val start_time = System.nanoTime
    val stay_time1 = System.nanoTime()
    data.map{case (longitude, latitude) => geo.encode(longitude, latitude)}

    val stay_time2 = System.nanoTime()
    data.map{case (longitude, latitude) => geo2.encode(longitude, latitude)}
    val end_time = System.nanoTime()


    println(stay_time2 - stay_time1, end_time - stay_time2)

    "152419098"
    "1498647235"
    // Test SparkContext.
//    val rdd = sc.parallelize((0 until 100).toList)
//    val rdd_sum = rdd.reduce(_ + _)
//    println(rdd_sum)



  }

}
