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

// Java
import java.util.UUID

// ScalaTest
import org.scalatest.matchers.MustMatchers
import org.scalatest.FreeSpec

// This project
import DynamoTestDataObjects.DynamoTestObject
import nonblocking.{Read, Save}

class DynamoReaderMonadTest extends FreeSpec with MustMatchers with DynamoTestObjectSupport{

  "Save/Get" in {
    val obj = DynamoTestObject(UUID.randomUUID().toString, "some test value" + math.random)

    val saved : Option[DynamoTestObject] = for {
      _ <- Save(obj)
      saved <- Read[DynamoTestObject](obj.id)
    } yield saved

    assert(saved.get === obj)
  }

}

