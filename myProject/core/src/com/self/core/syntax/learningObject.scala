package com.self.core.syntax

import com.self.core.baseApp.myAPP


object learningObject extends myAPP with Serializable {
  override def run: Unit = {
    /**
      * class和object的基本方法
      */

/*    // Traversable, Iterable
    class Singular[A](element: A) extends Traversable[A]{
      // 如果继承trait——Traversable，必须顶一个foreach方法
      def foreach[B](fun: A => B) = fun(element)
    }
    val p = new Singular("Plane")
    p.foreach(println)
    p.foreach(x => println(x.length))
    println(p.head)

    // overloaded
    class OverLoader{
      def print(s: String): Unit = println(s"String: $s")
      def print(s: Int): Unit = println(s"Int: $s")
      def print(s: Seq[String]): Unit = println(s"String: ${s.mkString(", ")}")
    }
    val loader = new OverLoader
    loader.print(1)
    loader.print("1")
    loader.print("1" :: "2" :: "3" :: Nil)

    // import as a new name
    import scala.collection.mutable.{Map => muMap}
    val mp = Map((1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five"))
    val muMp = muMap((1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five"))
    muMp += (6 -> "six")
    muMp.foreach(println)*/



    /**
      * access modifier
      */
/*   // protect for value
    class User{
      protected val passwd = "123456"
    }

    class newUser extends User{
      val newPasswd = passwd
    }

    val user = new User
    val newUser = new newUser
    println(newUser.newPasswd)

    class Filler(protected val passwd: String){}
    val filler = new Filler("123456")
//    println(filler.passwd) // not compile

    class newFiller{
      protected val passwd = "123456"
      protected def proprintlnPasswd(): Unit = println(passwd)
      def printlnPasswd(): Unit = println(passwd)
    }

    val newfiller = new newFiller
    println(newfiller.printlnPasswd())
    //    println(newfiller.proprintlnPasswd()) // not compile*/


    // private

    import org.apache.spark.mllib.clustering.KMeans
    import org.apache.spark.mllib.feature.Normalizer

//    object testSerializable extends Serializable{
//      def main(args: Array[String]): Unit = {
//        val param = new learningPrivate()
//          .set_height(150.0)
//          .set_weight(171.5)
//          .set_name("DataShoe")
//
//        val rdd = sc.parallelize((1 until 100).toList)
//          .map(x => (x.toDouble, x.toDouble, x.toString))
//
////        rdd.map(_ => (171.5, 75.5, "DataShoe")).foreach(println)
//        param.changRdd(rdd)
//
//        val newRdd = learningPrivate.train("DataShoe", 171.5, 155.5, rdd)
//        newRdd.foreach(println)


    object Transposer{
      implicit class TransArr[T](val matrix: Array[Array[T]]){
        def transposeee(): Seq[Seq[T]] =
        {
          Array.range(0, matrix.head.length).map(i => matrix.view.map(_(i)))
        }
      }

      implicit class TransSeq[T](val matrix: Seq[Seq[T]]){
        def transposeee(): Seq[Seq[T]] =
        {
          Array.range(0, matrix.head.length).map(i => matrix.view.map(_(i)))
        }
      }
    }

    val matrix = Seq(Seq(0, 1, 0), Seq(0, 0, 1), Seq(1, 0, 0))
    matrix.foreach(arr => println(arr.mkString(", ")))

    // 转置
    import Transposer._
    matrix.transposeee().foreach(arr => println(arr.mkString(", ")))



    Array.range(0, 10).foreach(println)
    val s = Array.range(0, 10)
    s.slice(0, s.length).foreach(println)
  }

}
