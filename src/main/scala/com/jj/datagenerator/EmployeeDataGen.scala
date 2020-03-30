package com.jj.datagenerator

import com.jj.Util
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random.nextInt

class EmployeeDataGen(spark: SparkSession, empIDList:List[Int], minAge:Int, maxAge:Int) {

  var listFirstName:List[String] = List[String]()
  def getUniqueFirstNameList: String ={
    val charactersListForFirstName = "abcdefghijklmnopqrstuvwxyz"
    var name=""
    var uniqueNameFound = false
    while(!uniqueNameFound)
    {
      name = Util.randomStringFromCharList(4+nextInt(10),charactersListForFirstName)
      if(!listFirstName.contains(name)) {
        listFirstName = listFirstName :+ name
        uniqueNameFound = true
      }
    }
    name
  }

  def getEmployeeDF: DataFrame ={

    val seqFilled = Seq.fill(empIDList.length){
      (
        getUniqueFirstNameList,
        Util.getRandomLastNameList,
        Util.getRandomAge(minAge,maxAge)
      )
    }
    val seqWithId = empIDList zip seqFilled
    import spark.sqlContext.implicits._
    val df = seqWithId.map(r=>(r._1,r._2._1,r._2._2,r._2._3)).toDF("emp_id","firstName","lastName","age")
    df

  }
}
