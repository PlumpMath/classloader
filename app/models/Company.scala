package models

import norm._
import play.api.libs.json.Json
import norm.QueryOperation._

import scala.language.postfixOps

case class Company(id: Option[Long],
                   name: String,
                   enabled: Boolean = true) extends Norm[Company] {
  def enable() = {
    update('enabled -> true)
  }
}

object Company extends NormCompanion[Company] {

  implicit val companyFormat = Json.format[Company]

  def findEnabled(): List[Company] = {
    findByQueryCondition(QueryCondition("enabled", EQ, true), "name")
  }
}