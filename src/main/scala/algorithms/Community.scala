package algorithms

class Community(prefix: String, nShard: Int)
  extends Algorithm[Long](prefix, nShard, false, "") {
  def iterations = {
    Some(vertices.result)
  }
}
