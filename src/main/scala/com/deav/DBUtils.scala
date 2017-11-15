package com.deav


import java.sql.{DriverManager, ResultSet, Statement}


/**
  * Created by Administrator on 2017/9/22 0022.
  */
object DBUtils {
  def etl_selectcount(sql: String):Int = {
    // Change to Your Database Config
    val conn_str = Config("etl_db.url") + "&user="+Config("etl_db.username")+"&password="+Config("etl_db.password")
    // Load the driver
    classOf[com.mysql.jdbc.Driver]
    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    try{
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // Execute Query
      //清空数据库
      val rs = statement.executeQuery(sql)
      if (rs.next()) {
        rs.getInt(1)
      }else{
        0
      }
    }finally {
      conn.close
    }
  }
  def etl_update(sql: String) = {
    // Change to Your Database Config
    val conn_str = Config("etl_db.url") + "&user="+Config("etl_db.username")+"&password="+Config("etl_db.password")
    // Load the driver
    classOf[com.mysql.jdbc.Driver]
    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    try{
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // Execute Query
      //清空数据库
      statement.execute(sql)
    }finally {
      conn.close
    }
  }
  def etl_insert(sql: String):Int = {
    // Change to Your Database Config
    val conn_str = Config("etl_db.url") + "&user="+Config("etl_db.username")+"&password="+Config("etl_db.password")
    // Load the driver
    classOf[com.mysql.jdbc.Driver]
    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    var ret = 0
    try{
      // Configure to be Read Only
      val ps = conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS)
      // Execute Query
      ps.execute()
      val rs = ps.getGeneratedKeys()
      if (rs.next()) {
        ret = rs.getInt(1)
      }
    }finally {
      conn.close
    }
    ret
  }
}

