package configuration

/**
 * @author ShiZhan
 * Parallel: implements common objects for multi-threads
 */
object Parallel {
  import akka.actor.ActorSystem
  val sageActors = ActorSystem("SAGE")
}
