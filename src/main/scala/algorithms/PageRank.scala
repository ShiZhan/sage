package algorithms

class PageRank(nLoop: Int)(implicit ep: graph.EdgeProvider)
    extends Algorithm[Double] {
  import scala.collection.concurrent.TrieMap
  import graph.Edge

  val q = 0.15d
  val p = 1 - q
  val sum = TrieMap[Long, Double]()
  val deg = TrieMap[Long, Int]()

  def iterations = {
    logger.info("collecting vertex degree")

    ep.getEdges.foreach {
      case Edge(u, v) =>
        Seq(u, v).foreach { k => val d = deg.getOrElse(k, 0); deg(k) = d + 1; scatter(k, 1) }
    }

    val nVertex = data.size
    scatter.foreach { i => data(i) /= nVertex }
    val data0 = q / nVertex

    (1 to nLoop) foreach { l =>
      logger.info("Loop {}", l)
      ep.getEdges.foreach {
        case Edge(u, v) =>
          val s = sum.getOrElse(v, 0.0d)
          sum(v) = s + (data(u) / deg(u))
      }
      scatter.filter(sum.contains).foreach { i => data(i) = data0 + sum(i) * p; sum(i) = 0.0d }
    }
  }
}
