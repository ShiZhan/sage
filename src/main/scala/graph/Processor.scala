package graph

object Processor {
  import algorithms._

  def run(prefix: String, nShard: Int, algorithm: String) = {
    val shards = Shards(prefix, nShard)
    if (shards.intact) {
      algorithm.split(":").toList match {
        case "bfs" :: root :: Nil => new BFS(shards).run(root.toLong)
        case "bfs" :: "u":: root :: Nil => new BFS_U(shards).run(root.toLong)
        case "sssp" :: root :: Nil => new SSSP(shards).run(root.toLong)
        case "cc" :: Nil => new CC(shards).run
        case "community" :: Nil => new Community(shards).run
        case "cluster" :: Nil => new Cluster(shards).run
        case "pagerank" :: Nil => new PageRank(shards).run
        case "triangle" :: Nil => new Triangle(shards).run
        case "stat" :: Nil => new Stat(shards).run
        case "print" :: Nil => shards.getAllEdges.foreach(println)
        case _ => println(s"[$algorithm] not implemented")
      }
    } else println("edge list(s) incomplete")
  }
}
