package com.zeebox.dynamo

import org.scalatest.matchers.MustMatchers
import org.scalatest.FreeSpec

class DynamoObjectTest extends FreeSpec with MustMatchers{

  case class Tst(id :String, name: String, email: String)
  implicit val ss = DynamoObject.of3(Tst)

  "Generates DO for basic case class" in {
    val tst = Tst("12312321", "Piotr", "piotrga@gmail.com")
    println(ss.toDynamo(tst))
    assert(ss.fromDynamo(ss.toDynamo(tst)) == tst)
  }

  "Works with nulls" in {
    val tst2 = Tst("12312321", "Piotr", null)
    println(ss.toDynamo(tst2))
    assert(ss.fromDynamo(ss.toDynamo(tst2)) == tst2)
  }

}

