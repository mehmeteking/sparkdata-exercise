package com.example
package badguys

class Inspector(val session: Session) extends SparkProps {
  import org.apache.spark.sql.functions._
  import session.sqlContext.implicits._
  import Inspector._
  
  def investigate(businesses: DataFrame, inspections: DataFrame, violations: DataFrame): Dataset[Inspector.Result] = {
    val filtered = mergeFiltered(filterInspections(inspections), filterViolations(violations))
    val resultFrame = mergeWithBusiness(filtered, businesses)
    resultFrame.map(Result.FromRow)
  }

  def filterInspections: DataFrame => DataFrame =
    _.groupBy(col("business_id"))
      .agg(min(when(col("Score").isNotNull, col("Score"))
        .otherwise(lit(0))).as("score"))

  def filterViolations: DataFrame => DataFrame =
    _.withColumn("default_risk", lit(2))
      .groupBy(col("business_id"))
      .agg(max(
        when(col("risk_category").like("High Risk"), lit(2))
          .otherwise(when(col("risk_category").like("Moderate Risk"), lit(1))
            .otherwise(lit(0)))).as("risk"),
        collect_list(col("ViolationTypeID")).as("violations"))

  def mergeFiltered(inspections: DataFrame, violations: DataFrame): DataFrame =
    inspections.join(violations,
      inspections("business_id") === violations("business_id"), "inner")
      .select(inspections("business_id"), col("score"), col("risk"), col("violations"))

  def mergeWithBusiness(filtered: DataFrame, businesses: DataFrame): DataFrame =
    filtered.join(businesses, filtered("business_id") === businesses("business_id"), "inner")
      .select(filtered("business_id"), col("score"), col("risk"), col("violations"), col("name"))


}
object Inspector {

  case class Result(id: Long, name: String, score: Int, category: String, violations: Seq[Result.Violation])
  object Result {
    case class Violation(id: Int, count: Int)

    case object FromRow extends (Row => Result) {
      def apply(row: Row): Inspector.Result = {
        val id = row.getAs[String](0).toLong
        val score = row.getAs[String](1).toInt
        val risk = row.getAs[Int](2) match {
          case 0 => "Low Risk"
          case 1 => "Moderate Risk"
          case 2 => "High Risk"
        }
        val violations = row.getAs[Seq[String]](3)
          .map(_.toInt)
          .groupBy(i => i).map(t => Result.Violation(t._1, t._2.size))
        val name = row.getAs[String](4)
        Result(id, name, score, risk, violations.toSeq)
      }
    }
  }
}
