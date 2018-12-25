package com.self.core.learningGraphx

import com.self.core.baseApp.myAPP

/**
  * Created by dell on 2018/1/31.
  */
object LearningGraphx extends myAPP{
  override def run(): Unit = {
//    /** scala类型擦除问题 */
//    object Extractor {
//      def extract[T](list: List[Any]) = list.flatMap {
//        case element: T => Some(element)
//        case _ => None
//      }
//    }
//
//    val list: List[Any] = List(1, "string1", List(), "string2")
//    val result = Extractor.extract[String](list)
//    println(result) // List(1, string1, List(), string



    import java.io._

    import org.apache.spark.mllib.clustering.KMeansModel
    import org.apache.spark.mllib.linalg.Vectors

    val centers = Array(
      Vectors.dense(Array(1.0, 0.0, 2.0, 0.0, 3.0)),
      Vectors.dense(Array(1.0, 1.0, 1.0, 0.0, 0.0)),
      Vectors.dense(Array(1.0, 2.0, 3.0, 4.0, 5.0))
    )
    val model = new KMeansModel(centers)

    def serialize[T](o: T): Array[Byte] = {
      val bos = new ByteArrayOutputStream()//基于磁盘文件流的序列化
      val oos = new ObjectOutputStream(bos)
      oos.writeObject(o)
      oos.close()
      val array = bos.toByteArray
      bos.close()
      array
    }

    serialize(model)

    def deserialize[T](bytes: Array[Byte]): T = {
      val bis = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(bis)
      val obj = ois.readObject.asInstanceOf[T]
      bis.close()
      ois.close()
      obj
    }













  }
}
