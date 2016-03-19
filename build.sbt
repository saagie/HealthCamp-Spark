import _root_.sbt.Keys._

name := "spark-mongodb"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2" % "provided"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"

libraryDependencies += "com.stratio.datasource" % "spark-mongodb_2.10" % "0.10.0"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0"

resolvers += Resolver.sonatypeRepo("public")
