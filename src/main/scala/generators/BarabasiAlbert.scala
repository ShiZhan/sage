package generators

import java.util.concurrent.ThreadLocalRandom
import scala.collection.JavaConversions._
import scala.util.Random
import scala.collection.mutable.Set
import graph.{ Edge, SimpleEdge, EdgeProvider }

class BarabasiAlbert(scale: Int, m0: Int) extends EdgeProvider[SimpleEdge] {
  require(scale > 0 && scale < 23 && m0 > 0) // if no '-J-Xmx?g' specified, then 'scale < 25'

  val V = 1 << scale
  val degree = Array.fill(V)(0) // 2^22 * 4 Bytes = 16MB

  def vertices(size: Int) = Iterator.from(0).take(size)

  def neighbours(id: Int) =
    if (id < m0)
      Iterator[SimpleEdge]()
    else if (id == m0) {
      degree(id) = m0
      (0 to (m0 - 1)).map { i => degree(i) = 1; Edge(m0, i) }.toIterator
    } else {
      val found = Set[Int]()
      val range0 = (id - m0).toLong * m0 * 2
      for (i <- 1 to m0) {
        val seeds = vertices(id).filterNot(found.contains)
        val range = range0 - (0 /: found.toIterator.map(degree)) { _ + _ }
        var dice = (Random.nextLong.abs % range) + 1
        for (v <- seeds if dice > 0) {
          dice -= degree(v)
          if (dice <= 0) found.add(v)
        }
      }
      degree(id) = m0
      found.toIterator.map { n => degree(n) += 1; Edge(id, n) }
    }

  def getEdges = vertices(V).flatMap(neighbours)
}

class BarabasiAlbertSimplified(scale: Int, m0: Int) extends EdgeProvider[SimpleEdge] {
  require(scale > 0 && scale < 31 && m0 > 0)

  def neighbours(id: Int) = {
    val n = Set[Int]()
    if (id >= m0) while (n.size < m0) n.add(ThreadLocalRandom.current().nextInt(id))
    n.toIterator
  }

  def getEdges = for (
    g <- Iterator.from(m0).take((1 << scale) - m0).grouped(1 << 13);
    u <- g.par;
    v <- neighbours(u)
  ) yield Edge(u, v)
}

class BarabasiAlbertOverSimplified(scale: Int, m0: Int) extends BarabasiAlbertSimplified(scale, m0) {
  override def neighbours(id: Int) =
    ThreadLocalRandom.current().ints(m0, 0, id).iterator().map(_.toInt)
}
