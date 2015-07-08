package sage.test

object ParallelIterationTest {
  import java.util.concurrent.ThreadLocalRandom
  import java.security.MessageDigest
  import helper.Timing._

  def someOps = {
    val md = MessageDigest.getInstance("SHA1")
    val bytes = Array.fill(1 << 25)(0.toByte)
    ThreadLocalRandom.current().nextBytes(bytes)
    md.update(bytes)
    md.digest().map("%02x".format(_)).mkString
  }

  val r = 1 to 1 << 3

  def batchOps0 = () => for (i <- r) yield someOps
  def batchOps1 = () => for (i <- r.par) yield someOps
  def batchOps2 = () => r.par.map(_ => someOps)
  def batchOps3 = () => (1 to 1 << 4).par.map(_ => someOps)

  def main(args: Array[String]) = {
    println("items  elapsed (ms)")
    println("-----  ------------")
    Seq( batchOps0, batchOps1, batchOps2, batchOps3)
      .map { _.elapsed }
      .map { case (r, e) => "% 5d  % 12d".format(r.size, e) }
      .foreach(println)
  }
}
