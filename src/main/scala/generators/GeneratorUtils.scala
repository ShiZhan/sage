package generators

/**
 * @author Zhan
 * Generator utilities
 */
object GeneratorUtils {
  import scala.util.Random
  import graph.{ Edge, WEdge }

  implicit class WEdgeConverter[Edge](edges: Iterator[Edge]) {
    def toWEdges = edges.map { case Edge(u, v) => WEdge(u, v, Random.nextFloat) }
  }
}
