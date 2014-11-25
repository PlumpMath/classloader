package controllers

import scala.concurrent.ExecutionContext.Implicits.global
import models.Company
import play.api.libs.json.Json
import play.api.mvc._

import scala.concurrent.Future

object Application extends Controller {

  def listJson() = Action.async {
    implicit request => Future {
      val companyList: List[Company] = Company.findAll()
//      Ok(Json.obj("companyList" -> companyList)).as(JSON)
      Ok(Json.obj("companyList" -> companyList, "string" -> false)).as(JSON)
    }
  }

}