package graph

object Processor {
  import algorithms._

  def run(prefix: String, nShard: Int, algorithm: String) = {
    val shards = Shards(prefix, nShard)
    if (shards.intact) {
      val vertices = Vertices(prefix + "-vertices.db")
      algorithm.split(":").toList match {
        case "bfs" :: root :: Nil => new BFS(vertices, shards).run(root.toLong)
        case "sssp" :: root :: Nil => new SSSP(vertices, shards).run(root.toLong)
        case "cc" :: Nil => new CC(vertices, shards).run
        case "scc" :: Nil => new SCC(vertices, shards).run
        case "cluster" :: Nil => new Cluster(vertices, shards).run
        case "pagerank" :: Nil => new PageRank(vertices, shards).run
        case "triangle" :: Nil => new Triangle(vertices, shards).run
        case "stat" :: Nil => new Stat(vertices, shards).run
        case _ => shards.getAllEdges.foreach(println)
      }
      vertices.close()
    } else println("edge list(s) incomplete")
  }
}
