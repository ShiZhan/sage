name := "sage"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.6"

scalacOptions in Compile ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation", "-feature", "-unchecked",
  "-Xlint")

libraryDependencies ++= {
  val mapdbV = "2.0-beta1"
  val slf4jV = "1.7.12"
  Seq(
    "org.mapdb" % "mapdb"         % mapdbV,
    "org.slf4j" % "slf4j-api"     % slf4jV,
    "org.slf4j" % "slf4j-log4j12" % slf4jV,
    "log4j"     % "log4j"         % "1.2.17"
  )
}
