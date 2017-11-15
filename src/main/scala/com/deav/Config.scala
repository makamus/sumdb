package com.deav

import com.typesafe.config.ConfigFactory

/**
  * Created by Administrator on 2017/1/19 0019.
  */
object Config {
  private val conf = ConfigFactory.load()

  def apply(key:String)={
    conf.getString(key)
  }
}
