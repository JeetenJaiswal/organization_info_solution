package com.jj

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object QueryExecutor extends App{

  val sparkConf = new SparkConf().setMaster("local").setAppName("employeeInfoUseCase")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  DataFrameGenerator.populateAndRegisterTempTable

  //1. Write a program to find emp with age > 40 & ctc > 30,000

  val problem1Query = "select e.*, ef.ctc from employees e inner join " +
    "employee_finance ef " +
    "where e.emp_id = ef.emp_id and e.age > 40 and ef.ctc > 30000"
  val result1 = spark.sql(problem1Query)
  result1.show()

  //2. Write a program to find dept with max emp with age > 35 & gratuity < 800

  val problem2Query = "select e.emp_id, e.firstName, e.lastName, e.age, ef.ctc, ef.gratuity, d.department_id, d.department_name " +
    "from employee_department ed inner join employees e on ed.emp_id = e.emp_id " +
    "inner join departments d on ed.department_id = d.department_id " +
    "inner join employee_finance ef on ed.emp_id = ef.emp_id " +
    "where ef.gratuity < 800 and e.age > 35"

  val result2 = spark.sql(problem2Query)
  result2.show()

}
