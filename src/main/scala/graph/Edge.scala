package graph

case class Edge(u: Long, v: Long) extends Ordered[Edge] {
  def compare(that: Edge) =
    if (u != that.u) {
      if ((u - that.u) > 0) 1 else -1
    } else if (v != that.v) {
      if ((v - that.v) > 0) 1 else -1
    } else 0
  def selfloop = u == v
  def reverse = Edge(v, u)
  override def toString = s"$u $v"
}

case class EdgeFile(name: String) {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import Edges.{ edgeScale, edgeSize }
  import helper.Gauge.IteratorOperations

  val p = Paths.get(name)
  val fc = FileChannel.open(p, READ, WRITE, CREATE)
  val buf = ByteBuffer.allocate(edgeSize).order(ByteOrder.LITTLE_ENDIAN)

  def close = fc.close()
  def total = fc.size() >> edgeScale

  def putEdge(edge: Edge) = edge match {
    case Edge(u, v) =>
      buf.clear()
      buf.putLong(u)
      buf.putLong(v)
      buf.flip()
      fc.write(buf)
  }

  def put(edges: Iterator[Edge]) = edges.foreachDo { putEdge }

  def putRange(edges: Iterator[Edge], offset: Long) = {
    fc.position(offset << edgeScale)
    put(edges)
  }

  def putRange(edges: Iterator[Edge], offset: Long, count: Long) = {
    fc.position(offset << edgeScale)
    put(edges.take(count.toInt))
  }

  def get = {
    fc.position(0)
    Iterator.continually {
      buf.clear()
      val nBytes = fc.read(buf)
      if (nBytes == edgeSize) {
        buf.flip()
        val u = buf.getLong
        val v = buf.getLong
        Edge(u, v)
      } else {
        Edge(-1, -1)
      }
    }.takeWhile(_.u != -1)
  }

  def getRange(offset: Long, count: Long) = {
    var n = count
    fc.position(offset << edgeScale)
    Iterator.continually {
      n -= 1
      buf.clear()
      val nBytes = fc.read(buf)
      if (nBytes == edgeSize) {
        buf.flip()
        val u = buf.getLong
        val v = buf.getLong
        Edge(u, v)
      } else {
        Edge(-1, -1)
      }
    }.takeWhile(_ => n >= 0)
  }

  def getThenClose = {
    fc.position(0)
    Iterator.continually {
      buf.clear()
      val nBytes = fc.read(buf)
      if (nBytes == edgeSize) {
        buf.flip()
        val u = buf.getLong
        val v = buf.getLong
        Edge(u, v)
      } else {
        fc.close()
        Edge(-1, -1)
      }
    }.takeWhile(_.u != -1)
  }
}

object Edges extends helper.Logging {
//  import configuration.Options.getCache
  import helper.Lines

  val edgeScale = 4
  val edgeSize = 1 << edgeScale

  def line2edge(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line); None
    case from :: to :: Nil =>
      Some(Edge(from.toLong, to.toLong))
    case _ =>
      logger.error("invalid: [{}]", line); None
  }

  def fromLines(edgeFile: String) =
    Lines.fromFileOrConsole(edgeFile).map(line2edge).filter(_ != None).map(_.get)

  implicit class EdgesWrapper(edges: Iterator[Edge]) {
    def toText(edgeFile: String) = Lines.toFile(edges, edgeFile)

    def toFile(edgeFile: String) = { val f = EdgeFile(edgeFile); f.put(edges); f.close }

//    def mergeSort(groupScale: Int) = {
//      require(groupScale <= 22) // 1 << 22 * 16 Bytes = 64 MBytes
//      val groupSize = 1 << groupScale
//      val edgeCache = getCache
//      edges.grouped(groupSize).foreach { _.toArray.sorted.foreach(edgeCache.putEdge) }
//      val total = edgeCache.total
//      require((total >> groupScale) < Int.MaxValue)
//      val nGroup = (total >> groupScale).toInt
//      val blocks = (0 to nGroup).map { i =>
//        val offset = i << groupScale
//        val count = if (i == nGroup) total & (groupSize - 1) else groupSize
//        edgeCache.getRange(offset, count)
//      }
//      Iterator.continually {
//
//      }
//      edgeCache.close
//      edges // TODO: merge sort
//    }
//
//    def uniq = {
//      var prev = Edge(-1, -1)
//      edges.filter { elem => if (elem != prev) { prev = elem; true } else false }
//    }
  }
}