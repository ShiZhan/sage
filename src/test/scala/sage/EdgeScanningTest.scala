package sage.test.Edge

object EdgeScanningTest {
  import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
  import graph.{ Edge, Edges, EdgeFile }
  import Edges._

  val V = 1 << 15
  val E = 1 << 22
  def eEdges = { var v = -1; Iterator.continually { v += 1; Edge(v, v + 1) }.take(E) }
  def sEdges(split: Int) = eEdges.grouped(E >> split)
  def edgeFileName(total: Int, id: Int) = s"$total-$id.bin"
  val system = ActorSystem("SAGE")

  def main(args: Array[String]) = {
    val nCase = 4
    (0 to (nCase - 1)).map(sEdges).zipWithIndex.foreach {
      case (edgeGroups, c) =>
        val total = c + 1
        edgeGroups.zipWithIndex.foreach {
          case (edges, id) =>
            val name = edgeFileName(total, id)
            edges.toIterator.toFile(name)
        }
        val edgeFiles = (0 to c).map(id => s"$total-$id.bin").map(EdgeFile)
        val edges = edgeFiles.map(_.get)
    }
  }
}
