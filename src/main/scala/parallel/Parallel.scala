/**
 * ActorSystem common objects 
 */
package parallel

/**
 * @author ShiZhan
 * Parallel: ActorSystem common objects
 */
object Parallel {
  import akka.actor.ActorSystem
  val sageActors = ActorSystem("SAGE")
}
