package graph

/**
 * @author Zhan
 * SimpleEdge:   simple edge
 * WeightedEdge: weighted edge
 * Edge:         common Edge object
 * EdgeStorage:  interface for edge storing classes
 * EdgeProvider: interface for edge loading classes
 */
abstract class Edge(u: Int, v: Int) {
  override def toString = s"$u $v"
}

class SimpleEdge(u: Int, v: Int) extends Edge(u, v) {
  val from = u
  val to = v
  def selfloop = u == v
  def reverse = new SimpleEdge(v, u)
}

class WeightedEdge(u: Int, v: Int, w: Float) extends SimpleEdge(u, v) {
  val weight = w
  override def reverse = new WeightedEdge(v, u, w)
  override def toString = s"$u $v $w"
}

object Edge {
  def apply(u: Int, v: Int) = new SimpleEdge(u, v)
  def apply(u: Int, v: Int, w: Float) = new WeightedEdge(u, v, w)
  def unapply(e: SimpleEdge) = Some((e.from, e.to))
  def unapply(e: WeightedEdge) = Some((e.from, e.to, e.weight))
}

trait EdgeStorage[E <: Edge] {
  def putEdges(edges: Iterator[E])
}

trait EdgeProvider[E <: Edge] {
  def getEdges: Iterator[E]
}