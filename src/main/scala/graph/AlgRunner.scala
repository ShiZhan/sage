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
    lazy val engine = new Engine(edgeFileNames, outputFileName)
    lazy val engine_w = new Engine_W(edgeFileNames, outputFileName)

    algOpt.split(":").toList match {
      case "bfs" :: root :: Nil => engine.run(new BFS(root.toInt))
      case "bfs" :: "u" :: root :: Nil => engine.run(new BFS_U(root.toInt))
      case "sssp" :: root :: Nil => engine_w.run(new SSSP(root.toInt))
      case "sssp" :: "u" :: root :: Nil => engine_w.run(new SSSP_U(root.toInt))
      case "cc" :: Nil => engine.run(new CC)
      case "kcore" :: Nil => engine.run(new KCore)
      case "pagerank" :: nLoop :: Nil => engine.run(new PageRank(nLoop.toInt))
      case "degree" :: Nil => engine.run(new Degree)
      case "degree" :: "u" :: Nil => engine.run(new Degree_U)
      case _ => println(s"[$algOpt] unknown")
    }
  }
}
