import sbt._

object Dependencies {

  //  Spark
  val sparkVersion = "2.4.3"
  val sparkTestVersion = "2.4.3_0.12.0"
  val jacksonVersion = "2.9.7"

  val sparkDependencies: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-avro" % sparkVersion % "provided",
    "com.holdenkarau" %% "spark-testing-base" % sparkTestVersion % "test",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "test"
  )

  val jacksonDependencies: Seq[ModuleID] = Seq(
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
  )

}
