package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.spark.rdd.RDD

import java.io.File
import scala.io.{Codec, Source}
import scala.util.Properties.isWin
import scala.util.Random


object StackOverflowSuite:
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)

class StackOverflowSuite extends munit.FunSuite:
  import StackOverflowSuite.*
  lazy val lines = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")

  // <postTypeId>,<id>,[<acceptedAnswer>],[<parentId>],<score>, [<tag>]
  lazy val toyPosts = sc.parallelize(
    Seq(
      Posting(1, 1, Some(3), None, 1, Some("Scala")),
      Posting(1, 2, Some(4), None, 2, Some("Python")),
      Posting(1, 3, Some(4), None, 3, Some("Python")),
      Posting(2, 4, None, Some(3), 4, Some("Python")),
      Posting(2, 5, None, Some(1), 5, Some("Scala")),
      Posting(2, 6, None, Some(2), 6, Some("Scala")),
    )
  )

  lazy val toyGroupedPostings = testObject.groupedPostings(toyPosts)
  lazy val toyScoredPostings = testObject.scoredPostings(toyGroupedPostings)
  lazy val toyVectorPostings = testObject.vectorPostings(toyScoredPostings)


  lazy val testObject = new StackOverflow {
    // Override values based on toy dataset
    override val langs = List("Scala", "Python")
    override def langSpread = 500
    override def kmeansKernels = 2
    override def kmeansEta: Double = 5.0D
    override def kmeansMaxIterations = 20
  }

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

  /**
   * Asserts that all the elements in an expected list are contained in a given list,
   * regardless of order. For a prettier output, given and expected should be sorted
   * with the same ordering.
   * @param actual The actual list
   * @param expected The expected list
   * @tparam A Type of the list elements
   */

  def assertContainedElements[A](actual: List[A], expected: List[A]): Unit =
    val actualSet = actual.toSet
    val expectedSet = expected.toSet
    val isSubset = expectedSet.subsetOf(actualSet)
    assert(isSubset == true)

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("test toy groupPostings") {
    val toyGroupedPostings = testObject.groupedPostings(toyPosts)
    assertEquals(toyGroupedPostings.count().toInt, 3)
    assertSameElements(
      toyGroupedPostings.keys.collect().toList,
      List(1, 2, 3)
    )
  }

  test("test toy scoredPostings") {
    assertSameElements(
      toyScoredPostings.collect().toList,
      List(
        (Posting(1,1,Some(3),None,1,Some("Scala")),5),
        (Posting(1,2,Some(4),None,2,Some("Python")),6),
        (Posting(1,3,Some(4),None,3,Some("Python")),4)
      )
    )
  }

  test("test toy vectorPostings") {
    println("------------------------")
    assertSameElements(
      toyVectorPostings.collect().toList,
      List((0, 5), (500, 6), (500, 4))
    )
  }

  test("test toy kmeans") {
    val toyMeans = testObject.kmeans(testObject.sampleVectors(toyVectorPostings), toyVectorPostings, debug = true)
    val results = testObject.clusterResults(toyMeans, toyVectorPostings)
    // 2 Kernels, 2 languages, 2 groups
    assertEquals(toyMeans.size, 2)
  }
