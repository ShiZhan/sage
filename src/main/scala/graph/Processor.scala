package graph

/**
 * @author Zhan
 * graph processer
 * algorithm: algorithm name and parameters
 * edgeFile:  input edge list
 * vdbFile:   KV file for storing immediate and final vertex value
 * nScan:     process with multiple threads
 */
object Processor {
  import algorithms._
  import helper.Lines.LinesWrapper

  def formatter(elem: (Long, Any)) = elem match { case (k: Long, v: Any) => s"$k $v" }

  //  def run(edgeFile: String, vdbFile: String, nScan: Int, algorithm: String) = {
  //    implicit val context = Context(edgeFile, vdbFile, nScan)
  //    val a = algorithm.split(":").toList match {
  //      case "bfs" :: root :: Nil => new BFS(root.toLong)
  //      case "bfs" :: "u" :: root :: Nil => new BFS_U(root.toLong)
  //      case "sssp" :: root :: Nil => new SSSP(root.toLong)
  //      case "cc" :: Nil => new CC
  //      case "community" :: Nil => new Community
  //      case "cluster" :: Nil => new Cluster
  //      case "pagerank" :: Nil => new PageRank
  //      case "triangle" :: Nil => new Triangle
  //      case "kcore" :: Nil => new KCore
  //      case "degree" :: Nil => new Degree
  //      case "degree" :: "u" :: Nil => new Degree_U
  //      case "status" :: Nil => new Status
  //      case _ => new Status
  //    }
  //
  //    val outFile = edgeFile + "-" + algorithm.replace(':', '-') + ".out"
  //    a.run match {
  //      case Some(result) => result.map(formatter).toFile(outFile)
  //      case _ =>
  //    }
  //  }
  def run(edgeFile: String, nScan: Int, algorithm: String) = {
    implicit val context = Context(edgeFile, nScan)
    val a = algorithm.split(":").toList match {
      case "bfs" :: root :: Nil => new BFS(root.toLong)
      case "bfs" :: "u" :: root :: Nil => new BFS_U(root.toLong)
      case "sssp" :: root :: Nil => new SSSP(root.toLong)
      case "cc" :: Nil => new CC
      case "community" :: Nil => new Community
      case "cluster" :: Nil => new Cluster
      case "pagerank" :: Nil => new PageRank
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
  }
}