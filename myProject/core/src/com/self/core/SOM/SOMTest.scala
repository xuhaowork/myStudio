package com.self.core.SOM

import com.self.core.baseApp.myAPP

object SOMTest extends myAPP{
  override def run(): Unit = {
    println("test")


    //
    //    class  MySparkPartition(numParts: Int) extends Partitioner {
    //
    //      override def numPartitions: Int = numParts
    //
    //      /**
    //        * 可以自定义分区算法
    //        * @param key
    //        * @return
    //        */
    //      override def getPartition(key: Any): Int = {
    //        val domain = new java.net.URL(key.toString).getHost()
    //        val code = (domain.hashCode % numPartitions)
    //        if (code &lt; 0) {
    //          code + numPartitions
    //        } else {
    //          code
    //        }
    //      }
    //      override def equals(other: Any): Boolean = other match {
    //        case mypartition: MySparkPartition =&gt;
    //      mypartition.numPartitions == numPartitions
    //        case _ =&gt;
    //      false
    //      }
    //      override def hashCode: Int = numPartitions


    // monopoly_increasing

    val rdd = sc.parallelize(Seq((1.0, "A"), (2.0, "B"), (3.0, "C"), (4.0, "D"), (5.0, "E"), (6.0, "F"),
      (7.0, "G"), (8.0, "H"), (9.0, "I")), 3).map(_._1)



    import org.apache.spark.sql.functions.monotonicallyIncreasingId
    rdd.mapPartitionsWithIndex((i, iter) => iter.map((_, i))).foreach(println)



  }
}
