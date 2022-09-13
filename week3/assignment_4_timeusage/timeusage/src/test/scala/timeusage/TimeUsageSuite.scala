package timeusage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ColumnName, DataFrame, Row}

import org.apache.spark.sql.*
import org.apache.spark.sql.types.*
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*


import scala.util.Random
import scala.util.Properties.isWin

object TimeUsageSuite:
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)

class TimeUsageSuite extends munit.FunSuite:
  import TimeUsageSuite.*
  import scala3encoders.given

  val lines = sc.parallelize(List(1, 2, 3, 4))
  lazy val (columns, initDf) = TimeUsage.read("src/test/resources/timeusage/atussum.csv")
  lazy val (primaryNeedsColumns, workColumns, otherColumns) = TimeUsage.classifiedColumns(columns)
  lazy val summaryDf = TimeUsage.timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
  lazy val groupedTimeUsage = TimeUsage.timeUsageGrouped(summaryDf)
  lazy val groupedTimeUsageSql = TimeUsage.timeUsageGroupedSql(summaryDf)
  lazy val summaryDfCaseClass = TimeUsage.timeUsageSummaryTyped(summaryDf)
  lazy val groupedTimeUsageCaseClass = TimeUsage.timeUsageGroupedTyped(summaryDfCaseClass)


  /**
   * Creates a truncated string representation of a list, adding ", ...)" if there
   * are too many elements to show
   * @param l The list to preview
   * @param n The number of elements to cut it at
   * @return A preview of the list, containing at most n elements.
   */
  def previewList[A](l: List[A], n: Int = 10): String =
    if l.length <= n then l.toString
    else l.take(n).toString.dropRight(1) + ", ...)"

  /**
   * Asserts that all the elements in a given list and an expected list are the same,
   * regardless of order. For a prettier output, given and expected should be sorted
   * with the same ordering.
   * @param actual The actual list
   * @param expected The expected list
   * @tparam A Type of the list elements
   */
  def assertSameElements[A](actual: List[A], expected: List[A]): Unit =
    val givenSet = actual.toSet
    val expectedSet = expected.toSet

    val unexpected = givenSet -- expectedSet
    val missing = expectedSet -- givenSet

    val noUnexpectedElements = unexpected.isEmpty
    val noMissingElements = missing.isEmpty

    val noMatchString =
    s"""
           |Expected: ${previewList(expected)}
           |Actual:   ${previewList(actual)}""".stripMargin

    assert(noUnexpectedElements,
    s"""|$noMatchString
            |The given collection contains some unexpected elements: ${previewList(unexpected.toList, 5)}""".stripMargin)

    assert(noMissingElements,
    s"""|$noMatchString
            |The given collection is missing some expected elements: ${previewList(missing.toList, 5)}""".stripMargin)

  test("test Read Resource") {
    // 455 columns
    assertEquals(columns.length, 455)
    // Test dataset is the 100-line head of the full dataset
    // There are 99 rows (plus the header) included
    assertEquals(initDf.count().toInt, 99)
    initDf.printSchema()
  }

  test("test row") {
    println(TimeUsage.row(List("stringcol", "1", "2", "3")))
  }


  test("test classifiedColumns toy") {
    val columns = List("t1801", "t1803", "t1805", "t18")
    val (primaryNeedsColumns, workColumns, otherColumns) = TimeUsage.classifiedColumns(columns)

    assertSameElements(primaryNeedsColumns, List(col("t1801"), col("t1803")))
    assertSameElements(workColumns, List(col("t1805")))
    assertSameElements(otherColumns, List(col("t18")))
  }

  test("test classifiedColumns") {
    assertEquals(primaryNeedsColumns.length, 55)
    assertEquals(workColumns.length, 23)
    assertEquals(otherColumns.length, 346)
  }

  test("test timeUsageSummary") {
    assertEquals(summaryDf.count().toInt, 68)
    assertEquals(summaryDf.columns.length, 6)
    summaryDf.show()
  }

  test("test timeUsageGrouped") {
    assertEquals(groupedTimeUsage.count().toInt, 10)
    groupedTimeUsage.show()
  }

  test("test timeUsageGroupedSqlQuery") {
    assertEquals(groupedTimeUsageSql.count().toInt, 10)
    groupedTimeUsageSql.show()
  }

  test("test timeUsageSummaryTyped") {
    println("summaryDfCaseClass")
    assertEquals(groupedTimeUsageCaseClass.count().toInt, 10)
    groupedTimeUsageCaseClass.show()
  }


  test("test timeUsageSummary consistency") {

    val cols1 = groupedTimeUsage.columns
    val cols2 = groupedTimeUsageSql.columns
    val cols3 = groupedTimeUsageCaseClass.columns

    // Ensure all columns are the same
    assertSameElements (cols1.toList, cols2.toList)
    assertSameElements (cols2.toList, cols3.toList)

    // Validate that one of the columns is identical
    assertSameElements (
      groupedTimeUsage.select("other").collect.toList,
      groupedTimeUsageSql.select("other").collect.toList
    )

    assertSameElements (
      groupedTimeUsageSql.select("other").collect.toList,
      groupedTimeUsageCaseClass.select("other").collect.toList
    )
  }
