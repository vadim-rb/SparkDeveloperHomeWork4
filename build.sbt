ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.13"

lazy val root = (project in file("."))
  .settings(
    name := "spark-connectors"
  )


val testcontainersScalaVersion = "0.38.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.postgresql" % "postgresql" % "42.2.18",

  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % "test",
  "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion % "test",
)

Test / fork := true
