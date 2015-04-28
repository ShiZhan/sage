package graph

import java.io.File
import java.util.concurrent.ConcurrentNavigableMap
import org.mapdb.DBMaker

case class Vertex(value: Long, renew: Boolean) {
  override def toString = s"value: $value, renew: $renew"
}

object Vertices extends helper.Logging {
  private val db = DBMaker.newTempFileDB().closeOnJvmShutdown().make()
  private val vertices = db.getTreeMap("vertices").asInstanceOf[ConcurrentNavigableMap[Long, Vertex]]

  def get(id: Long) = vertices.get(id)
  def put(id: Long, vertex: Vertex) = vertices.put(id, vertex)

  def commit() = db.commit()
  def close() = db.close()
}
