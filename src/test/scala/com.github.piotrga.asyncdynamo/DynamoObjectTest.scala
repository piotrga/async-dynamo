/*
 * Copyright 2012 2ndlanguage Limited.
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

import org.scalatest.matchers.MustMatchers
import org.scalatest.FreeSpec
import scala.concurrent.duration._
import scala.language.postfixOps

// This project
import nonblocking.{Read, Save, CreateTable, TableExists}

class DynamoObjectTest extends FreeSpec with MustMatchers with DynamoSupport{

  case class Person(id :String, name: String, email: String)
  implicit val personDO = DynamoObject.of3(Person)

  "Generates DO for basic case class" in {
    val tst = Person("12312321", "Piotr", "piotrga@gmail.com")
    assert(personDO.fromDynamo(personDO.toDynamo(tst)) == tst)
  }

  "Works with nulls" in {
    val tst2 = Person("12312321", "Piotr", null)
    assert(personDO.fromDynamo(personDO.toDynamo(tst2)) == tst2)
  }

  "Save/Read of dynamic DynamoObject" in {
    val tst = Person("12312321", "Piotr", "piotrga@gmail.com")
    if (! TableExists[Person]()) CreateTable[Person](5,5).blockingExecute(dynamo, 1 minute)

    val saved : Option[Person] = Save(tst) andThen Read[Person](tst.id)
    saved.get must be(tst)
  }

}

