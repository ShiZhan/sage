package graph

object Processor extends helper.Logging {
  import algorithms._

  def run(prefix: String, nShard: Int, algorithm: String) = {
    val shards = Shards(prefix, nShard)
    if (shards.intact) {
      val vertices = Vertices(prefix + "-vertices.db")
      algorithm.split(":").toList match {
        case "bfs" :: root :: Nil => new BFS(vertices, shards).run(root.toLong)
        case "sssp" :: root :: Nil => new SSSP(vertices, shards).run(root.toLong)
        case "scc" :: Nil =>
        case "pagerank" :: Nil =>
        case "triangle" :: Nil =>
        case _ => shards.getAllEdges.foreach(println)
      }
      vertices.close()
    } else logger.error("edge list(s) incomplete")
  }
}
