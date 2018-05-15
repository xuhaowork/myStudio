package com.self.core.syntax

class A {
  def a(param1: Int, param2: Int, param3: Boolean = true): Int = {
    param1 + param2 + 1
  }
  def a(param1: Int, param2: Int): Int = {
    a(param1: Int, param2, true)
  }

}
