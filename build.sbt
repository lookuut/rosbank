name := "spark-sbt"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"
libraryDependencies += "org.specs2" %% "specs2-core" % "4.0.2" % "test"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.18.0"
libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
libraryDependencies += "com.meetup" %% "archery" % "0.4.0"

scalacOptions in Test ++= Seq("-Yrangepos")