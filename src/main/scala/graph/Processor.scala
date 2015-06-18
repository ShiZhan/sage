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

  def run(edgeFileName: String, algorithm: String) = {
    implicit lazy val ep = new SimpleEdgeFile(edgeFileName)
    implicit lazy val wep = new WeightedEdgeFile(edgeFileName)
    val a = algorithm.split(":").toList match {
      case "bfs" :: root :: Nil => new BFS(root.toLong)
      case "bfs" :: "u" :: root :: Nil => new BFS_U(root.toLong)
      case "sssp" :: root :: Nil => new SSSP(root.toLong)
      case "sssp" :: "w" :: root :: Nil => new SSSP_W(root.toLong)
      case "sssp" :: "uw" :: root :: Nil => new SSSP_UW(root.toLong)
      case "cc" :: Nil => new CC
      case "kcore" :: Nil => new KCore
      case "pagerank" :: nLoop :: Nil => new PageRank(nLoop.toInt)
      case "cluster" :: Nil => new Cluster
      case "triangle" :: Nil => new Triangle
      case "degree" :: Nil => new Degree
      case "degree" :: "u" :: Nil => new Degree_U
      case _ => new Degree_U
    }

    val result = a.run
    ep.close
    wep.close
    val outFileName = edgeFileName + "-" + algorithm.replace(':', '-') + ".out"
    if (!result.isEmpty)
      result.map { case (k: Long, v: Any) => s"$k $v" }.toFile(outFileName)
  }
}