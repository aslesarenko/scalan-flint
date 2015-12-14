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
}
