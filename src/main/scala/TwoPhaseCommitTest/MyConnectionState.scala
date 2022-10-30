package TwoPhaseCommitTest

import java.sql.Connection

/**
 * @author jiangfan
 * @date 2022/9/30 10:50
 */
//class MyConnectionState(val connection:Connection){
//  val connection:Connection=input
//}
case class MyConnectionState( connection:Connection) extends Serializable