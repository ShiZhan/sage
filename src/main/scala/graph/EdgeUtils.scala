package graph

/**
 * @author Zhan
 * Edge utilities
 */
object EdgeUtils {
  import scala.util.Random

  implicit class EdgeConverter(edges: Iterator[WeightedEdge]) {
    def toEdges = edges.map { case Edge(u, v, w) => Edge(u, v) }
  }

  implicit class WEdgeConverter(edges: Iterator[SimpleEdge]) {
    def toWEdges = edges.map { case Edge(u, v) => Edge(u, v, Random.nextFloat) }
  }
}
