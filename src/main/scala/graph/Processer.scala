package graph

object Processer extends helper.Logging {
  def run(prefix: String, nShard: Int, jobOpt: String) = {
    val shards = Shards(prefix, nShard)
    if (shards.intact) {
      val vertices = Vertices(prefix + "-vertices.db")
      shards.getArray.foreach { s =>
        println(s"processing [$s] with [$jobOpt]")
        s.getEdges.foreach { println }
      }
      vertices.close()
    }
    else
      logger.error("edge list(s) incomplete")
  }
}
