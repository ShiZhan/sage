package algorithms

class PageRank(nLoop: Int)(implicit ep: graph.EdgeProvider)
    extends Algorithm[Double] {
  import scala.collection.concurrent.TrieMap
  import graph.Edge
  import helper.HugeContainers.GrowingArray

  val q = 0.15d
  val p = 1 - q
  val sum = GrowingArray[Double](0.0d)
  val deg = GrowingArray[Int](0)

  def iterations = {
    logger.info("collecting vertex degree")

    ep.getEdges.foreach {
      case Edge(u, v) =>
        Seq(u, v).foreach { k => val d = deg(k); deg(k) = d + 1; scatter(k, 1) }
    }

    val nVertex = data.size
    scatter.foreach { i => data(i) /= nVertex }
    val data0 = q / nVertex

    (1 to nLoop) foreach { l =>
      logger.info("Loop {}", l)
      ep.getEdges.foreach {
        case Edge(u, v) =>
          val s = sum(v)
          sum(v) = s + (data(u) / deg(u))
      }
      scatter.foreach { i => data(i) = data0 + sum(i) * p; sum(i) = 0.0d }
    }
  }
}
