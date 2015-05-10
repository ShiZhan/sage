package generators

class BarabasiAlbert(scale: Int, m0: Int) {
  require(scale > 0 && scale < 23 && m0 > 0) // if no '-J-Xmx?g' specified, then 'scale < 25'
  import scala.util.Random
  import scala.collection.mutable.Set
  import graph.Edge

  val total = 1 << scale
  val degree = Array.fill(total)(0) // 2^22 * 4 Bytes = 16MB

  def vertices(size: Int) = {
    var vID = -1
    Iterator.continually { vID += 1; vID }.take(size)
  }

  def neighbours(id: Int) =
    if (id < m0)
      Iterator[Edge]()
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

  def getIterator = vertices(total).flatMap(neighbours)
}