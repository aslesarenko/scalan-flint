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
  lazy val pairT1 = fun { (in: Rep[(Array[Int], Array[Int])]) =>
    val Pair(in1, in2) = in
    val t1 = ArrayDF(in1)
    val t2 = ArrayDF(in2)
    val pairT = PairDF(t1, t2)
    pairT.filter(fun { p => p._1 > 10 })
         .toArray
  }
  lazy val pairT2 = fun { (in: Rep[((Array[Int], Array[Int]), (Array[Int], Array[Double]))]) =>
    val Pair(Pair(in1, in2), Pair(in3, in4)) = in
    val t1 = ArrayDF(in1)
    val t2 = ArrayDF(in2)
    val t3 = ArrayDF(in3)
    val t4 = ArrayDF(in4)
    val pairT1 = PairDF(t1, t2)
    val pairT2 = PairDF(t3, t4)
    pairT1.filter(fun { p => p._1 > 10 })
      .join(pairT2, fun((r1: Rep[(Int,Int)]) => r1._1), fun((r2: Rep[(Int, Double)]) => r2._1), 1000, 0)
      .toArray
  }
  lazy val shardedT1 = fun { in: Rep[Array[(Int,Int)]] =>
    val nShards = 4
    val arrays = SArray.repeat(nShards)(fun { node: Rep[Int] => in.filter(p => p._1.hashcode % nShards === node)})
    val shards: Rep[Array[DataFrame[(Int,Int)]]] = arrays.map(arr => ArrayDF(arr))
    val shardedDf = ShardedDF(nShards, fun {p: Rep[(Int, Int)] => p._1.hashcode % nShards}, shards)
    shardedDf.filter(fun { p => p._1 > 10 }).toArray
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
  test("pairT1") {
    val in1 = Array(3, 6, 9, 12, 15)
    val in2 = Array(11, 22, 33, 44, 55)
    compareOutputWithSequential(_.pairT1)((in1, in2))
  }
  test("pairT2") {
    val in1 = Array(3, 6, 9, 12, 15)
    val in2 = Array(11, 22, 33, 44, 55)

    val in3 = Array(3, 6, 9, 12, 15)
    val in4 = Array(1.1, 2.2, 3.3, 4.4, 5.5)
    compareOutputWithSequential(_.pairT2)(((in1, in2),(in3, in4)))
  }
  test("shardedT1") {
    val in = Array((3, 11), (6, 22), (9, 33), (12, 44), (15, 55))
    compareOutputWithSequential(_.shardedT1)(in)
  }
}
