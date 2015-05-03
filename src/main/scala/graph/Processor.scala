package graph

object Processor extends helper.Logging {
  def run(prefix: String, nShard: Int, algorithm: String) = {
    val shards = Shards(prefix, nShard)
    if (shards.intact) {
      val vertices = Vertices(prefix + "-vertices.db")
      algorithm.split(":").toList match {
        case "bfs" :: root :: Nil => new algorithms.BFS(vertices, shards).run(root.toLong)
        case "dfs" :: root :: Nil =>
        case "sssp" :: root :: Nil =>
        case "pagerank" :: Nil =>
        case _ => shards.getAllEdges.foreach(println)
      }
      vertices.close()
    } else logger.error("edge list(s) incomplete")
  }
}
