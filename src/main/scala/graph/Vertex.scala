package graph

import java.io.File
import java.util.concurrent.ConcurrentNavigableMap
import org.mapdb.DBMaker

case class Vertex(id: Long, value: Long, renew: Boolean, shard: Byte)

class Vertices(vertexfn: String) {
  private val db = DBMaker.newFileDB(new File(vertexfn)).closeOnJvmShutdown().make()
  private val vertices = db.getTreeMap("vertices").asInstanceOf[ConcurrentNavigableMap[Long, Vertex]]

  def commit() = db.commit()
  def close() = db.close()
}

object Vertices {
  def apply(vertexfn: String) = new Vertices(vertexfn)
}