package algorithms

case class DirectedDegree(i: Int, o: Int) {
  def ==(that: DirectedDegree) = this.i == that.i && this.o == that.o
  def !=(that: DirectedDegree) = this.i != that.i || this.o != that.o
  override def toString = s"$i $o"
}

class Degree(implicit context: Context)
    extends Algorithm[DirectedDegree](context, DirectedDegree(0, 0)) {
  import helper.IteratorOps.VisualOperations
  import graph.Edge

  def iterations = {
    logger.info("Counting vertex in and out degree ...")
    getEdges.foreachDo {
      case Edge(u, v) =>
        val DirectedDegree(uI, uO) = data(u)
        val DirectedDegree(vI, vO) = data(v)
        data(u, DirectedDegree(uI, uO + 1))
        data(v, DirectedDegree(vI + 1, vO))
    }
  }
}

class Degree_U(implicit context: Context)
    extends Algorithm[Long](context, 0) {
  import helper.IteratorOps.VisualOperations
  import graph.Edge

  def iterations = {
    logger.info("Counting vertex degree ...")
    getEdges.foreachDo {
      case Edge(u, v) =>
        val uD = data(u); data(u, uD + 1)
        val vD = data(v); data(v, vD + 1)
    }
  }
}