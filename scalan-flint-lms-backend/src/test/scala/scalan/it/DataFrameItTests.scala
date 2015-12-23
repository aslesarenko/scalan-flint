package scalan.it

import scalan.compilation.lms.CommunityBridge
import scalan.compilation.lms.uni.LmsCompilerUni
import scalan.compilation.lms.scalac.CommunityLmsCompilerScala
import scalan.flint.{DataFramesDslExp, DataFramesDslSeq, DataFramesDsl}

/**
  * Created by dkolmakov on 12/14/15.
  */

trait DataFrameItTestsProg extends DataFramesDsl {
  lazy val t1 = fun { (in: DF[(Int,Int)]) =>
    in.filter(fun { p => p._1 > 10 })
  }
  lazy val t2 = fun { in: Rep[(DataFrame[(Int,Int)], DataFrame[(Int,Double)])] =>
    val Pair(in1, in2) = in
    in1.filter(fun { p => p._1 > 10 })
      .join(in2, fun((r1: Rep[(Int,Int)]) => r1._1), fun((r2: Rep[(Int,Double)]) => r2._1), 1000, 0)
  }

  lazy val arrT1 = fun { in: Rep[Array[(Int,Int)]] =>
    val arrDf = ArrayDF(in)
    t1(arrDf).toArray
  }
  lazy val arrT2 = fun { (in: Rep[(Array[(Int,Int)], Array[(Int,Double)])]) =>
    val Pair(in1, in2) = in
    val arrDf1 = ArrayDF(in1)
    val arrDf2 = ArrayDF(in2)
    t2((arrDf1, arrDf2)).toArray
  }
  lazy val pairT1 = fun { (in: Rep[Array[(Int, Int)]]) =>
    val pairDf = PairDF(ArrayDF(in.map(p => p._1)), ArrayDF(in.map(p => p._2)))
    t1(pairDf).toArray
  }
  lazy val pairT2 = fun { (in: Rep[(Array[(Int, Int)], Array[(Int, Double)])]) =>
    val Pair(in1, in2) = in
    val pairT1 = PairDF(ArrayDF(in1.map(p => p._1)), ArrayDF(in1.map(p => p._2)))
    val pairT2 = PairDF(ArrayDF(in2.map(p => p._1)), ArrayDF(in2.map(p => p._2)))
    t2(pairT1, pairT2).toArray
  }
  lazy val shardedT1 = fun { in: Rep[Array[(Int,Int)]] =>
    val nShards = 4
    val arrays = SArray.repeat(nShards)(fun { node: Rep[Int] => in.filter(p => p._1.hashcode % nShards === node)})
    val shards: Rep[Array[DataFrame[(Int,Int)]]] = arrays.map(arr => ArrayDF(arr))
    val shardedDf = ShardedDF(nShards, fun {p: Rep[(Int, Int)] => p._1.hashcode % nShards}, shards)
    t1(shardedDf).toArray
  }
  lazy val shardedT2 = fun { in: Rep[(Array[(Int,Int)], Array[(Int, Double)])] =>
    val nShards = 4
    val Pair(in1, in2) = in
    val arrays1 = SArray.repeat(nShards)(fun { node: Rep[Int] => in1.filter(p => p._1.hashcode % nShards === node)})
    val arrays2 = SArray.repeat(nShards)(fun { node: Rep[Int] => in2.filter(p => p._1.hashcode % nShards === node)})
    val shards1: Rep[Array[DataFrame[(Int,Int)]]] = arrays1.map(arr => ArrayDF(arr))
    val shards2: Rep[Array[DataFrame[(Int,Double)]]] = arrays2.map(arr => ArrayDF(arr))
    val shardedDf1 = ShardedDF(nShards, fun {p: Rep[(Int, Int)] => p._1.hashcode % nShards}, shards1)
    val shardedDf2 = ShardedDF(nShards, fun {p: Rep[(Int, Double)] => p._1.hashcode % nShards}, shards2)
    t2(shardedDf1, shardedDf2).toArray
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

  val t1In = Array((3, 11), (6, 22), (9, 33), (12, 44), (15, 55))
  val t2In = (Array((3, 11), (6, 22), (9, 33), (12, 44), (15, 55)),
              Array((3, 1.1), (6, 2.2), (9, 3.3), (12, 4.4), (15, 5.5)))

  test("arrT1") {
    compareOutputWithSequential(_.arrT1)(t1In)
  }
  test("arrT2") {
    compareOutputWithSequential(_.arrT2)(t2In)
  }
  test("pairT1") {
    compareOutputWithSequential(_.pairT1)(t1In)
  }
  test("pairT2") {
    compareOutputWithSequential(_.pairT2)(t2In)
  }
  test("shardedT1") {
    compareOutputWithSequential(_.shardedT1)(t1In)
  }
  test("shardedT2") {
    compareOutputWithSequential(_.shardedT2)(t2In)
  }
}
