package graph

/**
 * @author Zhan
 * graph processor
 * algorithm:     algorithm name and parameters
 * edgeFileName:  input edge list
 */
object Processor {
  import algorithms._
  import helper.Lines.LinesWrapper

  def run(edgeFileName: String, algorithm: String) = {
    implicit val edgeFile = graph.EdgeFile(edgeFileName)
    val a = algorithm.split(":").toList match {
      case "bfs" :: root :: Nil => new BFS(root.toLong)
      case "bfs" :: "u" :: root :: Nil => new BFS_U(root.toLong)
      case "sssp" :: root :: Nil => new SSSP(root.toLong)
      case "cc" :: Nil => new CC
      case "kcore" :: Nil => new KCore
      case "pagerank" :: nLoop :: Nil => new PageRank(nLoop.toInt)
      case "community" :: Nil => new Community
      case "cluster" :: Nil => new Cluster
      case "triangle" :: Nil => new Triangle
      case "degree" :: Nil => new Degree
      case "degree" :: "u" :: Nil => new Degree_U
      case _ => new Degree_U
    }

    val result = a.run
    edgeFile.close
    val outFileName = edgeFileName + "-" + algorithm.replace(':', '-') + ".out"
    if (!result.isEmpty)
      result.map { case (k: Long, v: Any) => s"$k $v" }.toFile(outFileName)
  }
}