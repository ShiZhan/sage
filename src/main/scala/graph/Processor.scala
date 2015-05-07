package graph

object Processor {
  import algorithms._

  def run(prefix: String, nShard: Int, algorithm: String) = {
    val shards = Shards(prefix, nShard)
    if (shards.intact) {
      algorithm.split(":").toList match {
        case "bfs" :: root :: Nil => new BFS(shards).run(root.toLong)
        case "sssp" :: root :: Nil => new SSSP(shards).run(root.toLong)
        case "cc" :: Nil => new CC(shards).run
        case "cluster" :: Nil => new Cluster(shards).run
        case "pagerank" :: Nil => new PageRank(shards).run
        case "triangle" :: Nil => new Triangle(shards).run
        case "stat" :: Nil => new Stat(shards).run
        case _ => shards.getAllEdges.foreach(println)
      }
    } else println("edge list(s) incomplete")
  }
}
