package com.example
package badguys

class Inspector(val session: Session) extends SparkProps {

  
  def investigate(businesses: DataFrame, inspections: DataFrame, violations: DataFrame): Dataset[Inspector.Result] = {
    ???
  }

}
object Inspector {

  case class Result(id: Long, name: String, score: Int, category: String, violations: Seq[Result.Violation])
  object Result {
    case class Violation(id: Int, count: Int)
  }
}
