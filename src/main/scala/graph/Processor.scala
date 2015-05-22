package graph

object Processor {
  import algorithms._
  import helper.Lines.Lines2File

  def formatter(elem: (Long, Any)) = elem match { case (k: Long, v: Any) => s"$k $v" }

  def run(prefix: String, nShard: Int, vdbFile: String, algorithm: String) = {
    implicit val context = Context(prefix, nShard, vdbFile)
    val a = algorithm.split(":").toList match {
      case "bfs" :: root :: Nil => new BFS(root.toLong)
      case "bfs" :: "u" :: root :: Nil => new BFS_U(root.toLong)
      case "bfs" :: "r" :: root :: Nil => new BFS_R(root.toLong)
      case "sssp" :: root :: Nil => new SSSP(root.toLong)
      case "cc" :: Nil => new CC
      case "cc" :: "r" :: Nil => new CC_R
      case "community" :: Nil => new Community
      case "cluster" :: Nil => new Cluster
      case "pagerank" :: Nil => new PageRank
      case "triangle" :: Nil => new Triangle
      case "kcore" :: Nil => new KCore
      case "degree" :: Nil => new Degree
      case "degree" :: "u" :: Nil => new Degree_U
      case "status" :: Nil => new Status
      case _ => new Status
    }

    val outFile = prefix + "-" + algorithm.replace(':', '-') + ".out"
    val result = a.run
    if (result != None) result.get.map(formatter).toFile(outFile)
  }
}