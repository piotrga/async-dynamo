package com.github.piotrga.asyncdynamo
package functional

import org.scalatest.FreeSpec
import org.scalatest.matchers.MustMatchers
import Iteratee._
import scala.language.postfixOps

class IterateeTest extends FreeSpec with MustMatchers{

  val list = List(1, 5, 3, 9, 12)

  "takeAll" in {
    enumerate(list, takeAll[Int]()) must be (Done(list))
  }

  "takeOnly" in {
    enumerate(list, takeOnly[Int](3)) must be(Done(list.take(3)))
  }

  "takeUntil" in {
    enumerate(list, takeUntil[Int](_ == 5)) must be(Done(List(1)))
  }

  "takeWhile" in {
    enumerate(list, takeWhile[Int](_ != 5)) must be(Done(List(1)))
  }

  "map" in {
    enumerate(list, takeOnly[Int](2).map[Int](2 * _)) must be (Done(List(2, 10)))
  }

  "withFilter" in {
    enumerate(1 to 10 toList, takeOnly[Int](2).withFilter(_ % 2 == 0)) must be (Done(List(2, 4)))
  }

  "map withFilter" in {
    enumerate(1 to 10 toList, takeOnly[Int](2).map[Int](_*2).withFilter(_ % 2 == 0)) must be (Done(List(4, 8)))
  }
}
