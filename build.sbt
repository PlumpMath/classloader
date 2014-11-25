import play.PlayScala

name := """classloader"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.1"

resolvers ++= Seq(
  "github.99Taxis norm releases" at "https://raw.github.com/99taxis/norm/releases/"
)

libraryDependencies ++= Seq(
  javaJdbc,
  jdbc,
  cache,
  anorm,
  ws,
  "org.postgresql" % "postgresql" % "9.3-1102-jdbc41"
)
