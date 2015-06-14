package graph

/**
 * @author Zhan
 * graph processer
 * algorithm: algorithm name and parameters
 * edgeFile:  input edge list
 */
object Processor {
  import algorithms._
  import helper.Lines.LinesWrapper

  def formatter(elem: (Long, Any)) = elem match { case (k: Long, v: Any) => s"$k $v" }

  def run(edgeFileName: String, algorithm: String) = {
    implicit val edgeFile = graph.EdgeFile(edgeFileName)
    val a = algorithm.split(":").toList match {
      case "bfs" :: root :: Nil => new BFS(root.toLong)
      case "bfs" :: "u" :: root :: Nil => new BFS_U(root.toLong)
      case "sssp" :: root :: Nil => new SSSP(root.toLong)
      case "cc" :: Nil => new CC
      case "community" :: Nil => new Community
      case "cluster" :: Nil => new Cluster
      case "triangle" :: Nil => new Triangle
      case "kcore" :: Nil => new KCore
      case "degree" :: Nil => new Degree
      case "degree" :: "u" :: Nil => new Degree_U
      case _ => new Degree_U
    }

    val outFile = edgeFile + "-" + algorithm.replace(':', '-') + ".out"
    a.run match {
      case Some(result) => result.map(formatter).toFile(outFile)
      case _ =>
    }
    edgeFile.close
  }
}