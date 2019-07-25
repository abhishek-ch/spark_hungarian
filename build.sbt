

ThisBuild / name := "spark-hungarian"
ThisBuild / version := "0.1"
ThisBuild / organization := "com.abc.hungarian"
ThisBuild / scalaVersion := "2.11.12"

lazy val IntegrationTest = config("it").extend(Test)

lazy val sparkSettings = Defaults.itSettings ++ Seq(

  libraryDependencies ++= Dependencies.sparkDependencies,

  // we will still need to figure out for -latest
  publishConfiguration := publishConfiguration.value.withOverwrite(true),
  publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),

  test in assembly := {},

  // fork in Test needs to be set to true to use javaOptions, and also prevents running out of heap metaspace
  fork in Test := true,

  // override the configuration file used in tests to one located in /test/resources/application.test.conf
  javaOptions in Test ++= Seq(s"-Dconfig.file=${sourceDirectory.value}/test/resources/application.test.conf"),
  testOptions in Test += Tests.Argument("-oDF")

)

lazy val assemblySettings = Seq(
  // decreases memory usage
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheUnzip = true),
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheOutput = true),

  assemblyJarName in assembly := name.value,
  assemblyMergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
    case PathList("META-INF", xs@_*) => MergeStrategy.concat
    case "log4j.properties" => MergeStrategy.first
    case PathList("reference.conf") => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
)

lazy val hungarianSystem = Project("spark-hungarian", file("."))
  .aggregate(
    clientStrategy
  )
  .settings(
    publish / skip := true
  )

lazy val clientStrategy = Project("hungarian-client", file("hungarian-client"))
  .configs(IntegrationTest)
  .settings(sparkSettings)
  .settings(assemblySettings: _*)
  .settings(
    libraryDependencies ++= Dependencies.jacksonDependencies,
    mainClass in assembly := Some("com.abc.hungarian.Main"),
    assemblyOutputPath in assembly := file(s"target/spark-hungarian.jar")
  )
