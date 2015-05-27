name := "sage"

version := "1.0"

scalaVersion := "2.11.6"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature")

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor_2.11" % "2.3.11",
  "org.mapdb" % "mapdb" % "2.0-alpha1",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-log4j12" % "1.7.5",
  "log4j" % "log4j" % "1.2.17"
)