name := "scala_spark"

version := "0.1"

scalaVersion := "2.11.12"
sparkVersion := "2.4.0"

libraryDependencies ++= Seq(
  //       "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",

  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.0_0.14.0" % "test"
)

