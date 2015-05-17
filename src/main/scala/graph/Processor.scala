package graph

object Processor {
  import algorithms._
  import helper.Lines.Lines2File

  def run(prefix: String, nShard: Int, algorithm: String) = {
    val shards = new SimpleShards(prefix, nShard)

    if (shards.intact) {
      val result = algorithm.split(":").toList match {
        case "bfs" :: root :: Nil => new BFS(shards).run(root.toLong)
        case "bfs" :: "u" :: root :: Nil => new BFS_U(shards).run(root.toLong)
        case "sssp" :: root :: Nil => new SSSP(shards).run(root.toLong)
        case "cc" :: Nil => new CC(shards).run
        case "community" :: Nil => new Community(shards).run
        case "cluster" :: Nil => new Cluster(shards).run
        case "pagerank" :: Nil => new PageRank(shards).run
        case "triangle" :: Nil => new Triangle(shards).run
        case "kcore" :: Nil => new KCore(shards).run
        case "degree" :: Nil => new Degree(shards).run
        case "print" :: Nil => shards.getAllEdges.map(_.toString)
        case _ => println(s"[$algorithm] not implemented"); Iterator[String]()
      }
      result.toFile(prefix + "-" + algorithm.replace(':', '-') + ".out")
    } else println("edge list(s) incomplete")
  }
}
