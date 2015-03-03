lazy val overlap =
  (project in file(".")).
  settings(
    name := "overlap",
    organization := "com.spotright.overlap",
    version := "1.0-SNAPSHOT",
    scalaVersion := "2.11.5",
    libraryDependencies ++= Seq(
      "com.spotright.common" %% "common-core" % "3.6.0"
    )
  )
