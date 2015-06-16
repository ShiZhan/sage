package algorithms

case class DirectedDegree(i: Int, o: Int) {
  def ==(that: DirectedDegree) = this.i == that.i && this.o == that.o
  def !=(that: DirectedDegree) = this.i != that.i || this.o != that.o
  override def toString = s"$i $o"
}

class Degree(implicit ep: graph.EdgeProvider)
    extends Algorithm[DirectedDegree] {
  import helper.IteratorOps.VisualOperations
  import graph.Edge

  val default = DirectedDegree(0, 0)

  def iterations = {
    logger.info("Counting vertex in and out degree ...")
    ep.getEdges.foreachDo {
      case Edge(u, v) =>
        val DirectedDegree(uI, uO) = data.getOrElse(u, default)
        val DirectedDegree(vI, vO) = data.getOrElse(v, default)
        data(u) = DirectedDegree(uI, uO + 1)
        data(v) = DirectedDegree(vI + 1, vO)
    }
  }
}

class Degree_U(implicit ep: graph.EdgeProvider)
    extends Algorithm[Long] {
  import helper.IteratorOps.VisualOperations
  import graph.Edge

  def iterations = {
    logger.info("Counting vertex degree ...")
    ep.getEdges.foreachDo {
      case Edge(u, v) =>
        val uD = data.getOrElse(u, 0L); data(u) = uD + 1
        val vD = data.getOrElse(v, 0L); data(v) = vD + 1
    }
  }
}