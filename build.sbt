import AssemblyKeys._

assemblySettings

name := "marlin"

version := "0.2-SNAPSHOT"

libraryDependencies  ++= Seq(
            // for hadoop 2.4
//            "org.apache.hadoop" % "hadoop-client" % "2.4.0" % "provided",
            // for spark 1.1.x
            //"org.scalanlp" % "breeze_2.10" % "0.9",
            //"org.scalanlp" % "breeze-natives_2.10" % "0.9",
            //"org.apache.spark" %% "spark-core" % "1.1.0" % "provided",
            // for spark 1.2.x
            //"org.scalanlp" % "breeze_2.10" % "0.10",
            //"org.scalanlp" % "breeze-natives_2.10" % "0.10",
            //"org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
            // for spark 1.3
//	          "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
//            "org.scalanlp" % "breeze_2.10" % "0.11.1",
            "org.scalanlp" % "breeze-natives_2.10" % "0.11.1",
            "org.scalatest" %% "scalatest" % "1.9.1" % "test",
            "colt" % "colt" % "1.2.0",
            "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "netlib Repository" at "http://repo1.maven.org/maven2/"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

scalaVersion := "2.10.4"

mergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    case PathList("org", "apache", xs@_*) => MergeStrategy.first
    case PathList("scala", "reflect", xs@_*) => MergeStrategy.first
    case x => old(x)
  }
}
