package graph

/**
 * @author Zhan
 * Edge utilities
 */
object EdgeUtils {
  import scala.util.Random
  import graph.{ Edge, WEdge }

  implicit class WEdgeConverter[Edge](edges: Iterator[Edge]) {
    def toWEdges = edges.map { case Edge(u, v) => WEdge(u, v, Random.nextFloat) }
  }
}
