package usecasetests

import com.jj.DataFrameGenerator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class DataGeneratorTests extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  var sparkConf:SparkConf=_
  var spark:SparkSession=_
  override def beforeAll(): Unit = {
    sparkConf = new SparkConf().setMaster("local").setAppName("employeeInfoUseCase")
    spark = SparkSession.builder().config(sparkConf).getOrCreate()
    DataFrameGenerator.populateAndRegisterTempTable
  }

  test("employees should have rows") {
    assert(spark.sql("select * from employees").count > 0)
  }

  test("employees firstName should be unique"){

    val employeeDataDFRowCount = spark.sql("select * from employees").count
    val employeeFirstNameCount = spark.sql("select distinct firstName from employees").count
    assert(employeeDataDFRowCount == employeeFirstNameCount)

  }
  test("employee age should be between 18 and 60"){

    val employeeAgeCheck = spark.sql("select * from employees e where e.age < 18 or e.age > 60").count
    assert(employeeAgeCheck == 0)

  }

  test("employee CTC should be between 10000 and 100000") {

    val employee_finance = spark.sql("select * from employee_finance ef where ef.ctc < 10000 or ef.ctc > 100000").count
    assert(employee_finance==0)
  }


  test("500 employee should be in first two department"){

    val empCountByDepartmentName = spark.sql("select * from employees e " +
      "inner join employee_department ed on e.emp_id=ed.emp_id " +
      "inner join departments d on ed.department_id = d.department_id where d.department_name in ('IT', 'INFRA')").count
    assert(empCountByDepartmentName==500)

  }

}
