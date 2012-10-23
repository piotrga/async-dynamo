package com.zeebox.dynamo

import org.scalatest.matchers.MustMatchers
import org.scalatest.FreeSpec
import akka.util.duration._

class DynamoObjectTest extends FreeSpec with MustMatchers with DynamoSupport{

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

  "Save/Read of dynamic DynamoObject" in {
    import DbOperation._
    val tst = Tst("12312321", "Piotr", "piotrga@gmail.com")
    if (!TableExists[Tst]().blockingExecute) CreateTable[Tst](5,5).blockingExecute(dynamo, 1 minute)

    val saved = (for {
      _ <- Save(tst)
      saved <- Read[Tst]("12312321")
    } yield saved).blockingExecute

    saved.get must be(tst)
  }

}

