package cn.datashoe.examples

import org.scalatest.FunSuite


/**
  * spark调用过程中的一些知识点
  */
class SparkEvoke extends FunSuite {
  /**
    * mapPartition中Iterator的迭代
    * ----
    * 利用Iterator直接定义一个新的Iterator
    * 1)不用转存为数组等结构, 防止数据过大栈溢出
    * 2)限制是不适合前后调用跨度特别大的mapPartition函数
    */
  test("iterator取差集") {
    import cn.datashoe.sparkBase.TestSparkAPP
    val a = TestSparkAPP.sc.parallelize(1 to 20, 2)

    def diff1(iter: Iterator[Int]): Iterator[Int] = {
      if (iter.isEmpty)
        Iterator[Int]()
      else {
        var value = iter.next()
        new Iterator[Int] {
          def hasNext: Boolean = iter.hasNext

          def next(): Int = {
            val lastOne = value
            value = iter.next()
            value - lastOne
          }
        }
      }
    }

    /*
    def diff2(iter: Iterator[Int]): Iterator[Int] = {
      val arr = iter.toArray
      arr.sliding(2).map {
        s =>
          s(1) - s(0)
      }
    }
    */

    /**
      * 结果解析
      * ----
      * 第1个分区
      * (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      * 结果
      * (1, 1, 1, 1, 1, 1, 1, 1, 1)
      *
      * 第2个分区
      * (11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
      * 结果
      * (1, 1, 1, 1, 1, 1, 1, 1, 1)
      */
    println("原数据")
    a.mapPartitionsWithIndex {
      case (partitionId, iter) =>
        Iterator((partitionId, iter.mkString(", ")))
    }.foreach(println)

    println("差分后数据")
    a.mapPartitions(diff1).mapPartitionsWithIndex {
      case (partitionId, iter) =>
        Iterator((partitionId, iter.mkString(", ")))
    }.foreach(println)


  }


  /**
    * spark rdd排序源码解读
    */
  test("排序源码解读") {
    import cn.datashoe.sparkBase.TestSparkAPP
    val a = TestSparkAPP.sc.parallelize(1 to 20, 2)
    a.sortBy(s => -s).take(10).foreach(println)
    a.top(10)

    // sortBy源于sortByKey
    // sortByKey会经过如下步骤:
    // 1)创建一个分区器，将数据分为不同的分区
    // val part = new RangePartitioner(numPartitions, self, ascending)
    // 2)
    // new ShuffledRDD[K, V, V](self, part)
    //      .setKeyOrdering(if (ascending) ordering else ordering.reverse)




  }


  test("top源码解读") {
    import cn.datashoe.sparkBase.TestSparkAPP
    val a = TestSparkAPP.sc.parallelize(1 to 20, 2)
    a.top(10)


  }


  test("goole guava排序器的使用") {
    /** 注意使用了 [[scala.collection.JavaConverters]] 该类的使用会在**/
    import scala.collection.JavaConverters._
    import scala.collection.JavaConverters.asJavaIteratorConverter

    import com.google.common.collect.{Ordering => GuavaOrdering}
    // 注意到takeOrdered使用了com.google.common.collect.Ordering的比较器
    object Utils {

      /**
        * Returns the first K elements from the input as defined by the specified implicit Ordering[T]
        * and maintains the ordering.
        */
      def takeOrdered[T](input: Iterator[T], num: Int)(implicit ord: Ordering[T]): Iterator[T] = {
        val ordering = new GuavaOrdering[T] {
          override def compare(l: T, r: T): Int = ord.compare(l, r)
        }
        ordering.leastOf(input.asJava, num).iterator.asScala
      }
    }
  }

}
