package com.zeebox.functional

/**
 * This reader monad in combination with CassandraOperation makes Cassandra operations composable, so we can do:
 * {{{op1 flatMap(r => op2(r))}}}
 * or:
 * {{{
 *   val userId = ...
 *   val person = ...
 *   val addFriend = for {
 *      account <- Read[Account](userId)
 *      facebookFriends <- Read[FacebookGraph](account.facebookId)
 *      _ <- Save(IsFollowing(account.id, person.id)) if ! (facebookFriends contains (person))
 *   } yield Unit
 *
 *   addFriend executeOn(cassandra)
 *
 *
 * }}}
 *
 * See this video for 30 minute explanation of moands http://www.youtube.com/watch?v=Mw_Jnn_Y5iA
 * and 30 minute explanation of reader monad http://www.youtube.com/watch?v=ZasXwtTRkio

 * @tparam C external resource we want to do the side-effects on. ie. Cassandra database.
 * @tparam A result type of the monad.
 */
trait ReaderMonad[C, A] {
  protected def apply(c: C): A
  def map[B](g: A => B): ReaderMonad[C, B] = (c: C) => g(apply(c))
  def flatMap[B](g: A => ReaderMonad[C, B]): ReaderMonad[C, B] = (c: C) => g(apply(c)).apply(c)
  def >>[B](g: => ReaderMonad[C,B]) : ReaderMonad[C,B] = flatMap(_ => g)
  def withFilter(p: A => Boolean ): ReaderMonad[C,A] = throw new RuntimeException("withFilter Not implemented")
}

object ReaderMonad {
  /**
   * This implicit lets us use more concise notation when defining Reader monad.
   * It just converts a function C => A to a Reader[C, A]
   */
  implicit def reader[C, A](f: C => A) : ReaderMonad[C,A] = new ReaderMonad[C, A] {override def apply(c: C): A = f(c)}


  /**
   * Let's say you have a list of items to read, and you have their keys in the list, and you know how to read one item by key.
   * If you want to create operation which does it all in one go you can use traverse:
   * {{{
   *   def readByKey(key:Int) = Get("items", key.toString, "col1", "col2") map(toItem)
   *   val keys = List(1, 50, 123)

   *   val readAll = traverse(keys)(readByKey)

   *   val res : List[Item] = readAll.execute(database)
   * }}}
   *
   *
   * @return operation which is a combination of operation g applied to all the elements of the list.
   * @see [[http://doc.akka.io/docs/akka/2.0.2/scala/futures.html Akka documentation ]]  for similar traverse operation for futures.
   */
  def traverse[C, A, B](list: List[A])(g: A => ReaderMonad[C, B]) : ReaderMonad[C, List[B]] = list match{
    case Nil => (c:C) => Nil
    case head :: tail => (c:C) => g(head).apply(c) :: (traverse(tail)(g)).apply(c)

  }
}