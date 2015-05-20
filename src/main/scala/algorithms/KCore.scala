package algorithms

class KCore(prefix: String, nShard: Int)
    extends Algorithm[Long](prefix, nShard, false, "") {
  import scala.collection.JavaConversions._
  import graph.Edge
  import helper.Gauge.IteratorOperations

  def iterations = {
    logger.info("Preparing vertex degree ...")
    val pool = vertices.getVertexTable("pool")
    def degreeIncreaseByOne(k: Long) = { val d = pool.getOrDefault(k, 0); pool.put(k, d + 1) }
    shards.getAllEdges.foreachDo { case Edge(u, v) => Seq(u, v).foreach(degreeIncreaseByOne) }

    var core = 1L
    val temp = vertices.getVertexTable("temp")
    while (!pool.isEmpty) {
      logger.info("Collecting core {}", core)
      if (pool.find { case (k, v) => v <= core } == None) core += 1
      else {
        pool.filter { case (k, v) => v <= core }
          .foreach { case (k, v) => pool.remove(k); temp.put(k, core) }
        shards.getAllEdges
          .flatMap { e => Iterator(e, e.reverse) }
          .filter { case Edge(u, v) => pool.containsKey(u) && temp.containsKey(v) }
          .foreach { case Edge(u, v) => val value = pool.get(u); pool.put(u, value - 1) }
        data.putAll(temp); temp.clear()
      }
    }
  }
}