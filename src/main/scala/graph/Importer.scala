package graph

object Importer {
  import helper.Gauge.IteratorOperations

  implicit class AlmostUniqIterator[T](iterator: Iterator[T]) {
    import scala.collection.mutable.Set
    val sSize = 1024 * 1024 // 16 MB each
    val aSize = 16 // 256 MB total, a 16 M items window for checking duplicates
    val setArray = Array.fill(aSize)(Set[T]())
    var current = 0
    def put(elem: T) = {
      if (setArray(current).size > sSize) {
        current = (current + 1) % aSize
        setArray(current).clear()
      }
      setArray(current).add(elem)
    }
    def contain(elem: T) = !setArray.par.forall { !_.contains(elem) }
    def almostUniq = iterator.map { elem =>
      if (contain(elem))
        None
      else {
        put(elem)
        Some(elem)
      }
    }.filter(_ != None).map(_.get)
  }

  implicit class EdgeMirroring(edges: Iterator[Edge]) {
    def toBidirection = edges.flatMap { e => Seq(e, Edge(e.v, e.u)).toIterator }
  }

  def run(edgeFile: String, nShard: Int, uniq: Boolean, bidirection: Boolean) = {
    val shards = Shards(edgeFile, nShard)
    val edges0 = EdgeUtils.fromFile(edgeFile)
    val edgesB = if (bidirection) edges0.toBidirection else edges0
    val edgesU = if (uniq) edgesB.almostUniq else edgesB

    edgesU.foreachDo { e => shards.getShardByVertex(e.u).putEdge(e) }

    shards.close
  }
}
