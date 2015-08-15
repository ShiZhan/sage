package settings

/**
 * @author ShiZhan
 * parsing config file
 */
object Config {
  import java.io.File
  import com.typesafe.config.{ Config, ConfigFactory }

  val config = ConfigFactory.parseFile(new File("sage.conf")) match {
    case c: Config if c.isEmpty() => ConfigFactory.parseResources("sage.conf")
    case c: Config => c
  }
  val nBuffersPerScanner = config.getInt("buffer.nBuffersPerScanner")
  val nEdgesPerBuffer = config.getInt("buffer.nEdgesPerBuffer")
}
