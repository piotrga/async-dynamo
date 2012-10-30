package asyncdynamo

package object functional {
  /**
   * Useful for paging, ie:
   * {{{
  def execute(implicit cassandra: ActorRef, timeout: Duration) : Stream[(String,String)]= {
    Streams.unfold(""){
      (marker : String) =>
        val res = nonblocking.GetSlice(columnFamilyId, rowId, marker, "", batchSize).executeOn(cassandra).get.toSeq
        val elements = if (res.headOption.isDefined && marker == res.head._1)
          res.tail
        else res

        val newMarker = elements.lastOption.map(_._1)
        (newMarker, elements)

    }.flatten
  }
  }}}
   * @param initial initial state, ie. for paging it could be empty marker or number zero for first page.
   * @param f transforms some state to the next state and result, ie. in batching for a given marker(state) it produces collection of elements(batch) and next marker(new state)
   * @tparam STATE type of the state, ie. for paging it could be a String.
   * @tparam ELEMENT type of the element, ie. for paging it could be a Seq[A]
   * @return stream of elements, ie. for paging it will be Stream[ Seq[A] ], so if you flatten it you will get Stream[A].
   *
   */
  def unfold[STATE, ELEMENT](initial: STATE)(f : STATE => (Option[STATE], ELEMENT)) : Stream[ELEMENT] = {
    (f(initial) match {
      case (Some(s), batch) => Stream.cons(batch, unfold(s)(f))
      case (None, list) => Stream(list)
    })
  }


}
