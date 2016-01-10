package graph

/**
 * @author Zhan
 * graph processor
 * algorithm:     algorithm name and parameters
 * edgeFileName:  input edge list
 */
object AlgRunner {
  import Parallel.{ Engine, Engine_W }
  import algorithms._

  def run(edgeFileNames: Array[String], algOpt: String, outputFileName: Option[String]) = {
    val oFN = outputFileName.getOrElse(edgeFileNames.head + "-" + algOpt.replace(':', '-') + ".csv")
    lazy val engine = new Engine(edgeFileNames, oFN)
    lazy val engine_w = new Engine_W(edgeFileNames, oFN)

    algOpt.split(":").toList match {
      case "bfs" :: root :: Nil => engine.run(new BFS(root.toLong))
      case "bfs" :: "u" :: root :: Nil => engine.run(new BFS_U(root.toLong))
      case "bfs" :: "u" :: "v" :: root :: Nil => engine.run(new BFS_U_V(root.toLong))
      case "sssp" :: root :: Nil => engine_w.run(new SSSP(root.toLong))
      case "sssp" :: "u" :: root :: Nil => engine_w.run(new SSSP_U(root.toLong))
      case "cc" :: Nil => engine.run(new CC)
      case "kcore" :: Nil => engine.run(new KCore)
      case "pagerank" :: nLoop :: Nil => engine.run(new PageRank(nLoop.toInt))
      case "degree" :: Nil => engine.run(new Degree)
      case "degree" :: "u" :: Nil => engine.run(new Degree_U)
      case _ => println(s"[$algOpt] unknown")
    }
  }
}
