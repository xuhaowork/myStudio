package com.self.core.learningRDD

import com.self.core.baseApp.myAPP

object learning extends myAPP{
  override def run(): Unit = {
    /** 模拟数据 */
    val rd = new java.util.Random(123L)

    // 三个中心
    val clusters = Array(Array(1.1, 2.0, 3.5), Array(-1.5, 2.0, -3.5), Array(0.0, 0.0, 0.0))

    import org.apache.spark.mllib.linalg.Vectors
    val data = sc.parallelize(clusters.zipWithIndex.flatMap {
      case (arr, idx) => Array.tabulate(1000)(_ => (Vectors.dense(arr.map(_ + rd.nextGaussian())), idx))
    })


    val data2 = data.map{case (_, idx) => idx}
    data2.cache() // 没有action操作不会缓存数据

//    data2.count() // 此时CacheManager触发cache数据

    val data3 = data.map{case (vec, _) => vec}



    val a: Array[Product with Serializable] = Array(Tuple1(1), Tuple2(2, 3))



  }
}
