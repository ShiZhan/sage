package graph

/**
 * @author Zhan
 * graph processor
 * algorithm:     algorithm name and parameters
 * edgeFileName:  input edge list
 */
object Processor {
  import algorithms._
  import graph.{ SimpleEdgeFile, WeightedEdgeFile }
  import helper.Lines.LinesWrapper

  def run(edgeFileNames: List[String], algOpt: String, multiThread: Boolean) =
    if (multiThread) {
      runMultiThread(edgeFileNames, algOpt)
    } else {
      runSingleThread(edgeFileNames.head, algOpt)
    }

  def runSingleThread(edgeFileName: String, algOpt: String) = {
    implicit lazy val ep = new SimpleEdgeFile(edgeFileName)
    implicit lazy val wep = new WeightedEdgeFile(edgeFileName)

    val algorithm = algOpt.split(":").toList match {
      case "bfs" :: root :: Nil => new BFS(root.toInt)
      case "bfs" :: "u" :: root :: Nil => new BFS_U(root.toInt)
      case "sssp" :: root :: Nil => new SSSP(root.toInt)
      case "sssp" :: "u" :: root :: Nil => new SSSP_U(root.toInt)
      case "cc" :: Nil => new CC
      case "kcore" :: Nil => new KCore
      case "pagerank" :: nLoop :: Nil => new PageRank(nLoop.toInt)
      case "degree" :: Nil => new Degree
      case "degree" :: "u" :: Nil => new Degree_U
      case _ => new Degree_U
    }

    val result = algorithm.run
    ep.close
    wep.close
    val outFileName = edgeFileName + "-" + algOpt.replace(':', '-') + ".out"
    if (!result.isEmpty)
      result.map { case (k: Int, v: Any) => s"$k $v" }.toFile(outFileName)
  }

  def runMultiThread(edgeFileNames: List[String], algOpt: String) = {
    implicit lazy val eps = edgeFileNames.map(new SimpleEdgeFile(_))
    implicit lazy val weps = edgeFileNames.map(new WeightedEdgeFile(_))

    val algorithm = algOpt.split(":").toList match {
      case "bfs" :: root :: Nil => new parallel.BFS(root.toInt)
      case "bfs" :: "u" :: root :: Nil => new parallel.BFS_U(root.toInt)
      case "sssp" :: root :: Nil => new parallel.SSSP(root.toInt)
      case "sssp" :: "u" :: root :: Nil => new parallel.SSSP_U(root.toInt)
      case "cc" :: Nil => new parallel.CC
      case "kcore" :: Nil => new parallel.KCore
      case "pagerank" :: nLoop :: Nil => new parallel.PageRank(nLoop.toInt)
      case "degree" :: Nil => new parallel.Degree
      case "degree" :: "u" :: Nil => new parallel.Degree_U
      case _ => new parallel.Degree_U
    }

    val result = algorithm.run
    eps.foreach(_.close)
    weps.foreach(_.close)
    val outFileName = edgeFileNames.head + "-" + algOpt.replace(':', '-') + ".out"
    if (!result.isEmpty)
      result.map { case (k: Int, v: Any) => s"$k $v" }.toFile(outFileName)
  }
}
