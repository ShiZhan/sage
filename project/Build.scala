import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object MyBuild extends Build {
  lazy val commonSettings = Defaults.defaultSettings ++ Seq(
    version := "0.1-SNAPSHOT",
    organization := "com.simba",
    scalaVersion := "2.11.6",
    scalacOptions in Compile ++= Seq(
      "-encoding", "UTF-8",
      "-deprecation", "-feature", "-unchecked",
      "-Xlint"),
    libraryDependencies ++= {
      val slf4jV = "1.7.12"
      Seq(
        "org.slf4j" % "slf4j-api"     % slf4jV,
        "org.slf4j" % "slf4j-log4j12" % slf4jV,
        "log4j"     % "log4j"         % "1.2.17"
      )
    }
  )
  
  lazy val buildSettings = commonSettings

  lazy val sage = Project(
    id = "sage",
    base = file("."),
    settings = commonSettings ++ assemblySettings ++ Seq(
      javaOptions in Test ++= Seq(
        "-Xmx1g",
        "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m"),
      logLevel in assembly := Level.Warn
    )
  )
}
