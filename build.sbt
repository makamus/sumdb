
name := "sumdb"

version := "1.0"

scalaVersion := "2.10.5"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.3"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.40"

packAutoSettings
    