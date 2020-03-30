package com.jj


import com.jj.datagenerator.EmployeeDataGen
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf

import scala.util.Random._


object DataFrameGenerator {

  
  def populateAndRegisterTempTable {

    val sparkConf = new SparkConf().setMaster("local").setAppName("employeeInfoUseCase")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    //TODO - read these from config
    val rowCount = 1000
    val minAge = 18
    val maxAge = 60
    val minCTC = 10000
    val maxCTC = 100000
    val basicPercent = 20
    val pfPercent = 10
    val gratuityPercent = 5




    val empIDList = 1 to rowCount toList
    val deptIdList = 100 to 500 by 100 toList
    val deptList:List[String] = List("IT", "INFRA", "HR", "ADMIN", "FIN")
    val deptListWithID = deptIdList zip deptList
    import spark.sqlContext.implicits._
    val deptDF = deptListWithID.toDF("department_id", "department_name")
    deptDF.createOrReplaceTempView("departments")

    val empDataGen = new EmployeeDataGen(spark,empIDList,minAge,maxAge)
    val empDF = empDataGen.getEmployeeDF
    empDF.createOrReplaceTempView("employees")

    val emp_finDF =  get_Emp_financeDF(spark,empIDList,minCTC,maxCTC, basicPercent, pfPercent, gratuityPercent )
    emp_finDF.createOrReplaceTempView("employee_finance")

    val empId_deptID_DF = get_empId_deptID_DF(spark,empIDList,deptIdList,rowCount)
    empId_deptID_DF.createOrReplaceTempView("employee_department")

  }

  //Distribute employee ID into department.
  // Half employee in 2 department.. remaining half in 3 department
  def get_empId_deptID_DF(
                            spark:SparkSession,
                            empIDList:List[Int],
                            deptIdList:List[Int],
                            rowCount:Int
                         ):DataFrame= {
    val empID_deptID = empIDList.map(e => (e, if (e <= rowCount / 2) deptIdList(nextInt(2)) else deptIdList(2 + nextInt(3))))
    import spark.sqlContext.implicits._
    val empID_deptID_DF = empID_deptID.toDF("emp_id", "department_id")
    empID_deptID_DF
  }

  def get_Emp_financeDF(spark:SparkSession,
                        empIDList:List[Int],
                        minCTC:Int,
                        maxCTC:Int,
                        basicPercent:Int,
                        pfPercent:Int,
                        gratuityPercent:Int
                       ):DataFrame = {
    //employee_finance

    val emp_financeInfo = empIDList.map(x=>(x,Util.getRandomCTC(minCTC,maxCTC)))
      .map(r=>(r._1,r._2,Util.getCTCComponent(r._2,basicPercent),
        Util.getCTCComponent(r._2,pfPercent),
        Util.getCTCComponent(r._2,gratuityPercent)
      ))
    import spark.sqlContext.implicits._
    val emp_fin_df = emp_financeInfo.toDF("emp_id","ctc","basic","pf","gratuity")
    emp_fin_df
  }

}
