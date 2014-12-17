/*
 * Copyright 2012-2015 2ndlanguage Limited.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
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
