package graph

/**
 * @author Zhan
 * Edge utilities
 * edge2wedge: convert simple edge to weighted edge
 * wedge2edge: convert weighted edge to simple edge
 */
object EdgeUtils {
  def edge2wedge(e: SimpleEdge) = Edge(e.from, e.to, util.Random.nextFloat)
  def wedge2edge(e: WeightedEdge) = Edge(e.from, e.to)
}