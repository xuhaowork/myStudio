package com.self.core.learningGraphx

import com.self.core.baseApp.myAPP

/**
  * Created by dell on 2018/1/31.
  */
object LearningGraphx extends myAPP{
  override def run(): Unit = {
    /** scala类型擦除问题 */
    object Extractor {
      def extract[T](list: List[Any]) = list.flatMap {
        case element: T => Some(element)
        case _ => None
      }
    }

    val list: List[Any] = List(1, "string1", List(), "string2")
    val result = Extractor.extract[String](list)
    println(result) // List(1, string1, List(), string













  }
}
