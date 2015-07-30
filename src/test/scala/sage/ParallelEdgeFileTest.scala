package sage.test

object ParallelEdgeFileTest {
  import graph.{ Edge, SimpleEdge, WeightedEdge, ParallelEngine }
  import ParallelEngine.{ Algorithm, Engine, Engine_W }
  import helper.GrowingArray
  import helper.Lines.LinesWrapper
  import helper.Timing._

  case class DirectedDegree(i: Int, o: Int) {
    def addIDeg = DirectedDegree(i + 1, o)
    def addODeg = DirectedDegree(i, o + 1)
    override def toString = s"$i $o"
  }

  class Degree extends Algorithm[SimpleEdge] {
    val degree = GrowingArray[DirectedDegree](DirectedDegree(0, 0))

    def compute(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges) degree.synchronized {
        degree(u) = degree(u).addODeg
        degree(v) = degree(u).addIDeg
      }

    def update() = {}

    def complete() =
      degree.synchronized { degree.updated.map { case (k, v) => s"$k $v" }.toFile("degree.csv") }
  }

  class Degree_U extends Algorithm[SimpleEdge] {
    val degree = GrowingArray[Int](0)

    def compute(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges) degree.synchronized {
        degree(u) = degree(u) + 1
        degree(v) = degree(v) + 1
      }

    def update() = {}

    def complete() =
      degree.synchronized { degree.updated.map { case (k, v) => s"$k $v" }.toFile("degree-u.csv") }
  }

  class BFS(root: Int) extends Algorithm[SimpleEdge] {
    val distance = GrowingArray[Int](0)
    var d = 1
    distance(root) = d
    gather.add(root)

    def compute(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges if (gather(u) && distance.unVisited(v))) distance.synchronized {
        distance(v) = d; scatter.add(v)
      }

    def update() = d += 1

    def complete() =
      distance.synchronized { distance.updated.map { case (k, v) => s"$k $v" }.toFile("bfs.csv") }
  }

  class BFS_U(root: Int) extends Algorithm[SimpleEdge] {
    val distance = GrowingArray[Int](0)
    var d = 1
    distance(root) = d
    gather.add(root)

    def compute(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges) distance.synchronized {
        if (gather(u) && distance.unVisited(v)) { distance(v) = d; scatter.add(v) }
        if (gather(v) && distance.unVisited(u)) { distance(u) = d; scatter.add(u) }
      }

    def update() = d += 1

    def complete() =
      distance.synchronized { distance.updated.map { case (k, v) => s"$k $v" }.toFile("bfs-u.csv") }
  }

  class CC extends Algorithm[SimpleEdge] {
    val component = GrowingArray[Int](Int.MaxValue)

    def compute(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges) component.synchronized {
        if (stepCounter == 0) {
          val min = u min v
          if (component(u) > min) { component(u) = min; scatter.add(u) }
          if (component(v) > min) { component(v) = min; scatter.add(v) }
        } else {
          if (gather(u)) {
            val value = component(u)
            if (value < component(v)) { component(v) = value; scatter.add(v) }
          }
          if (gather(v)) {
            val value = component(v)
            if (value < component(u)) { component(u) = value; scatter.add(u) }
          }
        }
      }

    def update() = {}

    def complete() =
      component.synchronized { component.updated.map { case (k, v) => s"$k $v" }.toFile("cc.csv") }
  }

  class KCore extends Algorithm[SimpleEdge] {
    val core = GrowingArray[Int](0)
    var c = 1

    def compute(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges) core.synchronized {
        if (stepCounter == 0) {
          core(u) = core(u) + 1; scatter.add(u)
          core(v) = core(v) + 1; scatter.add(v)
        } else if (gather(u) && gather(v)) {
          val dU = core(u)
          val dV = core(v)
          if (dU > c && dV > c) { scatter.add(u); scatter.add(v) }
          else if (dU > c && dV <= c) { core(u) = dU - 1; scatter.add(u) }
          else if (dU <= c && dV > c) { core(v) = dV - 1; scatter.add(v) }
        }
      }

    def update() = if (scatter.nonEmpty) c = scatter.view.map { core(_) }.min

    def complete() =
      core.synchronized { core.updated.map { case (k, v) => s"$k $v" }.toFile("kcore.csv") }
  }

  case class PRValue(value: Float, sum: Float, deg: Int) {
    def addDeg = PRValue(value, sum, deg + 1)
    def initPR(implicit nVertex: Int) = PRValue(1 / nVertex, sum, deg)
    def gather(delta: Float) = PRValue(value, sum + delta, deg)
    def scatter = value / deg
    def update(implicit nVertex: Int) = PRValue(0.15f / nVertex + sum * 0.85f, 0.0f, deg)
    override def toString = "%f".format(value)
  }

  class PageRank(nLoop: Int) extends Algorithm[SimpleEdge] {
    val pr = GrowingArray[PRValue](PRValue(0.0f, 0.0f, 0))
    implicit var nVertex = 0

    override def forward() = stepCounter += 1
    override def hasNext() = stepCounter <= nLoop

    def compute(edges: Iterator[SimpleEdge]) = if (stepCounter == 0) {
      for (Edge(u, v) <- edges) {
        pr(u) = pr(u).addDeg
        pr(v) = pr(v).addDeg
      }
    } else {
      for (Edge(u, v) <- edges) pr.synchronized {
        pr(v) = pr(v).gather(pr(u).scatter)
      }
    }

    def update() = if (stepCounter == 0) {
      logger.info("initialize PR value")
      nVertex = pr.nUpdated
      for ((id, value) <- pr.updated) pr(id) = value.initPR
    } else {
      logger.info("update PR value")
      for ((id, value) <- pr.updated) pr(id) = value.update
    }

    def complete() =
      pr.synchronized { pr.updated.map { case (k, v) => s"$k $v" }.toFile("pagerank.csv") }
  }

  class SSSP(root: Int) extends Algorithm[WeightedEdge] {
    val distance = GrowingArray[Float](Float.MaxValue)
    distance(root) = 0.0f
    gather.add(root)

    def compute(edges: Iterator[WeightedEdge]) =
      for (Edge(u, v, w) <- edges if (gather(u)))
        distance.synchronized {
          val d = distance(u) + w
          if (distance(v) > d) {
            distance(v) = d
            scatter.add(v)
          }
        }

    def update() = {}

    def complete() =
      distance.synchronized { distance.updated.map { case (k, v) => s"$k $v" }.toFile("sssp.csv") }
  }

  class SSSP_U(root: Int) extends Algorithm[WeightedEdge] {
    val distance = GrowingArray[Float](Float.MaxValue)
    distance(root) = 0.0f
    gather.add(root)

    def compute(edges: Iterator[WeightedEdge]) =
      for (Edge(u, v, w) <- edges) distance.synchronized {
        if (gather(u)) {
          val d = distance(u) + w
          if (distance(v) > d) {
            distance(v) = d
            scatter.add(v)
          }
        }
        if (gather(v)) {
          val d = distance(v) + w
          if (distance(u) > d) {
            distance(u) = d
            scatter.add(u)
          }
        }
      }

    def update() = {}

    def complete() =
      distance.synchronized { distance.updated.map { case (k, v) => s"$k $v" }.toFile("sssp-u.csv") }
  }

  def main(args: Array[String]) =
    if (args.nonEmpty) {
      val e0 = { () =>
        implicit val eps = args.map(new graph.WeightedEdgeFile(_)).toSeq
        val result = new algorithms.parallel.SSSP_U(0).run
        eps.foreach(_.close)
        result.map { case (k: Int, v: Any) => s"$k $v" }.toFile("sssp-u-reference.csv")
      }.elapsed
      println(s"reference run $e0 ms")
      new Engine_W(args).run(new SSSP_U(0))
    } else println("run with <edge file(s)>")
}
