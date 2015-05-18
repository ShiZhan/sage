package graph

object Processor {
  import algorithms._
  import helper.Lines.Lines2File

  def formatter(elem: (Long, Any)) = elem match { case (k: Long, v: Any) => s"$k $v" }

  def run(prefix: String, nShard: Int, algorithm: String) = {
    val a = algorithm.split(":").toList match {
      case "bfs" :: root :: Nil => new BFS(prefix, nShard, root.toLong)
      case "bfs" :: "u" :: root :: Nil => new BFS_U(prefix, nShard, root.toLong)
      case "bfs" :: "r" :: root :: Nil => new BFS_R(prefix, nShard, root.toLong)
      case "sssp" :: root :: Nil => new SSSP(prefix, nShard, root.toLong)
      case "cc" :: Nil => new CC(prefix, nShard)
      case "community" :: Nil => new Community(prefix, nShard)
      case "cluster" :: Nil => new Cluster(prefix, nShard)
      case "pagerank" :: Nil => new PageRank(prefix, nShard)
      case "triangle" :: Nil => new Triangle(prefix, nShard)
      case "kcore" :: Nil => new KCore(prefix, nShard)
      case "degree" :: Nil => new Degree(prefix, nShard)
      case "status" :: Nil => new Status(prefix, nShard)
      case _ => new Status(prefix, nShard)
    }

    val outFile = prefix + "-" + algorithm.replace(':', '-') + ".out"
    val result = a.run
    if (result != None) result.get.map(formatter).toFile(outFile)
  }
}