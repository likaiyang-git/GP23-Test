package com.myutils

/**
  * 给广告位贴标签
  */
trait TagsMake {
  //特质里边这个方法为了实现通用性，之后继承这个特质的类可以实现多个方法，
  // 在重写这个方法的时候可以再对参数进行强制类型转换
  def mkTags(args: Any*): List[(String, Int)]
}
