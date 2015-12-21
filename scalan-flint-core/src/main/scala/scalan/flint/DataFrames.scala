package scalan.flint

import scalan._
import scalan.collections._

trait DataFrames extends Base {
  self: DataFramesDsl =>

  type DF[T] = Rep[DataFrame[T]]

  trait DataFrame[T] extends Def[DataFrame[T]] {
    implicit def eT: Elem[T]

    /**
     * Filter input RDD
     */
    def filter(p: Rep[T => Boolean]): DF[T]

    /**
     * Perform map-reduce
     */
    def mapReduce[K:Elem,V:Elem](map: Rep[T => (K,V)], reduce: Rep[((V,V)) => V], estimation: Rep[Int]): DF[(K,V)]

    /**
     * Perform aggregation of input RDD
     */
    def reduce[S](accumulate: Rep[((S,T)) => S], combine: Rep[((S,S)) => S], initState: Rep[S]): DF[S]

    /**
     * Map records of input RDD
     */
    def project[P:Elem](projection: Rep[T => P]): DF[P]

    /**
     * Sort input RDD
     */
    def sort(compare: Rep[((T,T)) => Int], sizeEstimation: Rep[Int]): DF[T]

    /**
     * Sort input RDD
     */
    def sortBy(compare: Rep[Struct], sizeEstimation: Rep[Int]): DF[T]

    /**
     * Find top N records according to provided comparison function
     */
    def top(compare: Rep[((T,T)) => Int], n: Rep[Int]): DF[T]

    /**
     * Left join two RDDs
     */
    def join[I:Elem,K:Elem](innerRdd: DF[I], outerKey: Rep[T=>K], innerKey: Rep[I=>K],
                    estimation: Rep[Int], kind: Rep[Int]): DF[(T,I)]

    /**
     * Left simijoin two RDDs
     */
    def semijoin[I,K](innerRdd: DF[I], outerKey: Rep[T=>K], innerKey: Rep[I=>K],
                        estimation: Rep[Int], kind: Rep[Int]): DF[(T,I)]

    /**
     * Replicate data between all nodes.
     * Broadcast local RDD data to all nodes and gather data from all nodes.
     * As a result all nodes get the same replicas of input data
     */
    def replicate: DF[T]

    /**
     * Return single record from input RDD or substitute it with default value of RDD is empty.
     * This method is usful for obtaining aggregation result
     */
    def result(defaultValue: Rep[T]): Rep[T]

    /**
     * Print RDD records to the stream
     */
    def saveFile(fileName: String)

    /**
      * Converts DataFrame to an Array
      */
    def toArray: Rep[Array[T]]
  }
  trait DataFrameCompanion {}

  @InternalType
  trait FlintDataFrame[T] extends DataFrame[T] {

    /**
     * Filter input RDD
     * template<bool (*predicate)(T const&)>
     * RDD<T>* filter();
     */
    def filter(p: Rep[T => Boolean]): DF[T] = externalMethod("Rdd", "filter")

    /**
     * Perform map-reduce
     * template<class K,class V,void (*map)(Pair<K,V>& out, T const& in), void (*reduce)(V& dst, V const& src)>
     * RDD< Pair<K,V> >* mapReduce(size_t estimation);
     */
     def mapReduce[K:Elem,V:Elem](map: Rep[T => (K,V)], reduce: Rep[((V,V)) => V], estimation: Rep[Int]): DF[(K,V)] =
       externalMethod("Rdd", "mapReduce")

    /**
     * Perform aggregation of input RDD
     * template<class S,void (*accumulate)(S& state,  T const& in),void (*combine)(S& state, S const& in)>
     * RDD<S>* reduce(S const& initState);
     */
     def reduce[S](accumulate: Rep[((S,T)) => S], combine: Rep[((S,S)) => S], initState: Rep[S]): DF[S] =
      externalMethod("Rdd", "reduce")

    /**
     * Map records of input RDD
     * template<class P, void (*projection)(P& out, T const& in)>
     * RDD<P>* project();
     */
     def project[P:Elem](projection: Rep[T => P]): DF[P] =
      externalMethod("Rdd", "project")

    /**
     * Sort input RDD
     * template<int (*compare)(T const* a, T const* b)>
     * RDD<T>* sort(size_t estimation);
     */
     def sort(compare: Rep[((T,T)) => Int], sizeEstimation: Rep[Int]): DF[T] =
      externalMethod("Rdd", "sort")

    /**
     * Sort input RDD
     */
    def sortBy(compare: Rep[Struct], sizeEstimation: Rep[Int]): DF[T] =
      externalMethod("Rdd", "sortBy")

    /**
     * Find top N records according to provided comparison function
     * template<int (*compare)(T const* a, T const* b)>
     * RDD<T>* top(size_t n);
     */
    def top(compare: Rep[((T,T)) => Int], n: Rep[Int]): DF[T] =
      externalMethod("Rdd", "top")

    /**
     * Left join two RDDs
     * template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
     * RDD< Join<T,I> >* join(RDD<I>* with, size_t estimation, JoinKind kind = InnerJoin);
     */
     def join[I:Elem,K:Elem](innerRdd: DF[I], outerKey: Rep[T=>K], innerKey: Rep[I=>K],
                     estimation: Rep[Int], kind: Rep[Int]): DF[(T,I)] =
      externalMethod("Rdd", "join")

    /**
     * Left simijoin two RDDs
     * template<class I, class K, void (*outerKey)(K& key, T const& outer), void (*innerKey)(K& key, I const& inner)>
     * RDD<T>* semijoin(RDD<I>* with, size_t estimation, JoinKind kind = InnerJoin);
     */
     def semijoin[I,K](innerRdd: DF[I], outerKey: Rep[T=>K], innerKey: Rep[I=>K],
                         estimation: Rep[Int], kind: Rep[Int]): DF[(T,I)] =
      externalMethod("Rdd", "semijoin")

    /**
     * Replicate data between all nodes.
     * Broadcast local RDD data to all nodes and gather data from all nodes.
     * As a result all nodes get the same replicas of input data
     * virtual RDD<T>* replicate();
     */
     def replicate: DF[T] = externalMethod("Rdd", "replicate")

    /**
     * Return single record from input RDD or substitute it with default value of RDD is empty.
     * This method is usful for obtaining aggregation result
     * T result(T const& defaultValue)
     */
     def result(defaultValue: Rep[T]): Rep[T] = externalMethod("Rdd", "result")

    /**
     * Print RDD records to the stream
     * void output(FILE* out);
     */
     def saveFile(fileName: String) = externalMethod("Rdd", "output")

    /**
     * Converts DataFrame to an Array
     */
     def toArray: Rep[Array[T]] = !!!
  }

  abstract class FlintFileDF[T](val fileName: Rep[String])(implicit val eT: Elem[T]) extends DataFrame[T] with FlintDataFrame[T] {
  }
  trait FlintFileDFCompanion extends ConcreteClass1[FlintFileDF] {
  }

  abstract class InputDF[T](val dataSourceId: Rep[String])(implicit val eT: Elem[T]) extends DataFrame[T] with FlintDataFrame[T] {
  }
  trait InputDFCompanion extends ConcreteClass1[InputDF] {
  }

  abstract class PhysicalRddDF[T](val dataSourceId: Rep[String])(implicit val eT: Elem[T]) extends DataFrame[T] with FlintDataFrame[T] {
  }
  trait PhysicalRddDFCompanion extends ConcreteClass1[PhysicalRddDF] {
  }

  @InternalType
  trait TableDF[T] extends DataFrame[T] with FlintDataFrame[T] {
    /**
      * Filter input RDD
      */
    override def filter(p: Rep[T => Boolean]): DF[T] = ArrayDF(toArray.filterBy(p))

    /**
      * Perform map-reduce
      */
    override def mapReduce[K:Elem,V:Elem](map: Rep[T => (K,V)], reduce: Rep[((V,V)) => V], estimation: Rep[Int]): DF[(K,V)] = {
      ArrayDF(toArray.mapReduceBy[K,V](map, reduce).toArray)
    }

    /**
      * Map records of input RDD
      */
    override def project[P:Elem](projection: Rep[T => P]): DF[P] = {
      ArrayDF(toArray.mapBy(projection))
    }

    def joinTables[T:Elem, I:Elem, K:Elem](outer: DF[T], inner: DF[I], outKey: Rep[T => K], inKey: Rep[I => K]): DF[(T,I)] = {
      val map = MMultiMap.fromArray[K, I](inner.toArray.map(i => (inKey(i), i)))
      ArrayDF[(T, I)](outer.toArray.flatMap(o => map(outKey(o)).toArray.map(i => (o, i))))
    }

    /**
      * Left join two RDDs
      */
    override def join[I:Elem,K:Elem](innerRdd: DF[I], outerKey: Rep[T=>K], innerKey: Rep[I=>K],
                                     estimation: Rep[Int], kind: Rep[Int]): DF[(T,I)] = {
      joinTables[T,I,K](self, innerRdd, outerKey, innerKey)
    }
  }

  abstract class ArrayDF[T](val records: Rep[Array[T]])(implicit val eT: Elem[T]) extends DataFrame[T] with TableDF[T] {
    /**
      * Converts DataFrame to an Array
      */
    override def toArray = records
  }
  trait ArrayDFCompanion extends ConcreteClass1[ArrayDF] {
    def create[T:Elem](arr: Rep[Array[T]]): DF[T] = ArrayDF[T](arr)
  }

  abstract class PairDF[L, R](val left: DF[L], val right: DF[R])
                            (implicit val eL: Elem[L], val eR: Elem[R])
    extends DataFrame[(L, R)] with TableDF[(L, R)] {

    val eT = element[(L, R)]
    /**
      * Converts DataFrame to an Array
      */
    override def toArray = left.toArray zip right.toArray
  }
  trait PairDFCompanion extends ConcreteClass2[PairDF] {
    def create[L:Elem, R:Elem](left: DF[L], right: DF[R]): DF[(L, R)] = PairDF[L,R](left, right)
  }

  abstract class ShardedDF[T](val nShards: Rep[Int], val distrib: Rep[T => Int], val shards: Rep[Array[DataFrame[T]]])
                             (implicit val eT: Elem[T])
    extends DataFrame[T] with TableDF[T] {
    /**
      * Converts DataFrame to an Array
      */
    override def toArray = shards.fold[ArrayBuffer[T]](ArrayBuffer.empty[T], fun{ p: Rep[(ArrayBuffer[T], DataFrame[T])] => p._1 ++= p._2.toArray})
//    override def toArray = {
//      val lElem = toLazyElem(pairElement(element[ArrayBuffer[T]], element[DataFrame[T]]))
//      val f = fun { p: Rep[(ArrayBuffer[T], DataFrame[T])] => p._1 ++= p._2.toArray }(lElem, element[ArrayBuffer[T]])
//      array_fold[DataFrame[T], ArrayBuffer[T]](shards, ArrayBuffer.empty[T], f)
//    }
  }
  trait ShardedDFCompanion extends ConcreteClass1[ShardedDF] {
    def create[T:Elem](nShards: Rep[Int], createShard: Rep[Int => DataFrame[T]], distrib: Rep[T => Int]): DF[T] = {
      val shards = SArray.repeat[DataFrame[T]](nShards)(createShard)
      ShardedDF(nShards, distrib, shards)
    }
  }

}

trait DataFramesDsl extends CollectionsDsl with MultiMapsDsl with impl.DataFramesAbs {
}

trait DataFramesDslSeq extends CollectionsDslSeq with MultiMapsDslSeq with impl.DataFramesSeq { }

trait DataFramesDslExp extends CollectionsDslExp with MultiMapsDslExp with impl.DataFramesExp {

  def compareField(a: Rep[Struct], b: Rep[Struct], fieldName: String, ascending: Boolean): Rep[Int] = {
    if (ascending)
      compare(a(fieldName), b(fieldName), Nil)
    else
      compare(b(fieldName), a(fieldName), Nil)
  }

  def compareFields(a: Rep[Struct], b: Rep[Struct], fields: List[(String,Boolean)]): Rep[Int] = fields match {
    case Nil => !!!(s"Fields are not specified $fields")
    case ((fn, ascending) :: Nil) =>
      compareField(a, b, fn, ascending)

    case ((fn,ascending) :: t) =>
      val diff = compareField(a, b, fn, ascending)
      IF (diff !== 0) {
        diff
      } ELSE {
        compareFields(a,b,t)
      }
  }

  def compare[T](a: Rep[T], b: Rep[T], fields: List[(String, Boolean)]): Rep[Int] = {
    assert(a.elem == b.elem, s"compare is defined for elements of the same type: ${a.elem}, ${b.elem}")
    a.elem match {
      case se: StructElem[_] =>
        compareFields(a.asRep[Struct], b.asRep[Struct], fields)
      case BooleanElement => a.asRep[Boolean].toInt - b.asRep[Boolean].toInt
      case ByteElement => a.asRep[Byte].compare(b.asRep[Byte])
      case ShortElement => a.asRep[Short].compare(b.asRep[Short])
      case IntElement => a.asRep[Int].compare(b.asRep[Int])
      case CharElement => a.asRep[Char].compare(b.asRep[Char])
      case LongElement => a.asRep[Long].compare(b.asRep[Long])
      case FloatElement => a.asRep[Float].compare(b.asRep[Float])
      case DoubleElement => a.asRep[Double].compare(b.asRep[Double])
      case StringElement => a.asRep[String].compare(b.asRep[String])
      case e => !!!(s"Don't know how to compare values of type $e")
    }
  }

  def compareFun[T](fields: List[(String, Boolean)])(implicit eT: Elem[T]) = {
    fun { (in: Rep[(T,T)]) =>
      val Pair(a,b) = in
      compare(a,b,fields)
    }
  }
}

