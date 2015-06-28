package graph

/**
 * @author Zhan
 * Edge utilities
 */
object EdgeUtils {
  def edge2wedge(e: SimpleEdge) = Edge(e.from, e.to, util.Random.nextFloat)
  def wedge2edge(e: WeightedEdge) = Edge(e.from, e.to)
}