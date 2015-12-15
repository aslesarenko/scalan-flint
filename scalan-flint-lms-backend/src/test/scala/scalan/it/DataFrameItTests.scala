package scalan.it

import scalan.compilation.lms.CommunityBridge
import scalan.compilation.lms.uni.LmsCompilerUni
import scalan.compilation.lms.scalac.CommunityLmsCompilerScala
import scalan.flint.{DataFramesDslExp, DataFramesDslSeq, DataFramesDsl}

/**
  * Created by dkolmakov on 12/14/15.
  */
trait DataFrameItTestsProg extends DataFramesDsl {
  lazy val arrT1 = fun { in: Rep[Array[(Int,Int)]] =>
    val arrayDf = ArrayDF(in)
    arrayDf.filter(fun { p => p._1 > 10 }).toArray
  }
  lazy val arrT2 = fun { (in: Rep[(Array[(Int,Int)], Array[(Int,Double)])]) =>
    val Pair(in1, in2) = in
    val t1 = ArrayDF(in1)
    val t2 = ArrayDF(in2)
    t1.filter(fun { p => p._1 > 10 })
      .join(t2, fun((r1: Rep[(Int,Int)]) => r1._1), fun((r2: Rep[(Int,Double)]) => r2._1), 1000, 0)
      .toArray
  }
}

class DataFrameItTestsProgSeq extends DataFramesDslSeq with DataFrameItTestsProg

class DataFrameItTests extends BaseItTests[DataFrameItTestsProg](new DataFrameItTestsProgSeq) {
  class ScalanCake extends DataFramesDslExp with DataFrameItTestsProg {
    override val cacheElems = false
    //      override val cacheIsos = false
    //      override val cachePairs = false
  }

  val compiler = new CommunityLmsCompilerScala[ScalanCake](new ScalanCake) with CommunityBridge
  override val defaultCompilers = compilers(compiler)

  test("arrT1") {
    val in = Array((3, 11), (6, 22), (9, 33), (12, 44), (15, 55))
    compareOutputWithSequential(_.arrT1)(in)
  }
  test("arrT2") {
    val in1 = Array((3, 11), (6, 22), (9, 33), (12, 44), (15, 55))
    val in2 = Array((3, 1.1), (6, 2.2), (9, 3.3), (12, 4.4), (15, 5.5))
    compareOutputWithSequential(_.arrT2)((in1, in2))
  }
}
