package algorithms

case class DirectedDegree(i: Long, o: Long) {
  override def toString = s"$i $o"
}

class Degree(implicit context: Context)
    extends Algorithm[DirectedDegree](context) {
  import scala.collection.JavaConversions._
  import helper.IteratorOps.VisualOperations
  import graph.Edge

  def iterations = {
    logger.info("Counting vertex degree ...")
    getEdges.foreachDo {
      case Edge(u, v) =>
        val DirectedDegree(uI, uO) = data.getOrElse(u, DirectedDegree(0, 0))
        val DirectedDegree(vI, vO) = data.getOrElse(v, DirectedDegree(0, 0))
        data.put(u, DirectedDegree(uI, uO + 1))
        data.put(v, DirectedDegree(vI + 1, vO))
    }
  }
}

class Degree_U(implicit context: Context)
    extends Algorithm[Long](context) {
  import scala.collection.JavaConversions._
  import helper.IteratorOps.VisualOperations
  import graph.Edge

  def iterations = {
    logger.info("Counting vertex degree ...")
    var nEdges = 0L
    getEdges.foreachDo {
      case Edge(u, v) =>
        nEdges += 1
        val uD: Long = data.getOrElse(u, 0); data.put(u, uD + 1)
        val vD: Long = data.getOrElse(v, 0); data.put(v, vD + 1)
    }
    val nVertices = data.size
    logger.info(s"Vertices: $nVertices")
    logger.info(s"Edges:    $nEdges")
  }
}