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

package asyncdynamo

import com.amazonaws.services.dynamodbv2.model._
import scala.Some
import scala.Tuple2

trait DynamoObject[T]{

  protected implicit def toS(value : String) = new AttributeValue().withS(value)
  protected def toN[A: Numeric](number: A) =  new AttributeValue().withN(number.toString)

  type KeyDefinition = Tuple2[String,String]
  protected def hashKey: KeyDefinition
  protected def rangeKey: Option[KeyDefinition] = None

  protected def table : String
  def toDynamo(t:T) : Map[String, AttributeValue]
  def fromDynamo(attributes: Map[String, AttributeValue]) : T
  def table(prefix: String): String = prefix + table

  def hashSchema: KeySchemaElement = new KeySchemaElement().withAttributeName(hashKey._1).withKeyType(KeyType.HASH)
  def hashAttrib: AttributeDefinition = new AttributeDefinition().withAttributeName(hashKey._1).withAttributeType(hashKey._2)

  def rangeSchema: Option[KeySchemaElement] = rangeKey.map(key => Some(new KeySchemaElement().withAttributeName(key._1).withKeyType(KeyType.RANGE))).getOrElse(None)
  def rangeAttrib: Option[AttributeDefinition] = rangeKey.map(key => Some(new AttributeDefinition().withAttributeName(key._1).withAttributeType(key._2))).getOrElse(None)

  def asHashAttribute(v: Any): AttributeValue = DynamoObject.asAttribute(v, hashKey._2)
  def asRangeAttribute(v: Any): AttributeValue = DynamoObject.asAttribute(v, rangeKey.getOrElse(sys.error("This table doesn't have range attribute"))._2)
}

object DynamoObject {

  /**
   * Generates DynamoObject for a case class with one field. ie.
   * {{{
   * case class Abc(id: String)
   * implicit val abcDO = DynamoObject.of1(Abc)
   * Save(Abc("123124)")
   * }}}
   * @param construct case class
   * @return object extending DynamoObject trait which can be used as implicit with dynamo operations.
   */
  def of1[T <: Product :ClassManifest](construct : String => T) = apply((args : Seq[String]) => construct(args(0)))
  def of2[T <: Product :ClassManifest](construct : (String, String) => T) = apply((args : Seq[String]) => construct(args(0), args(1)))
  def of3[T <: Product :ClassManifest](construct : (String, String, String) => T) = apply((args : Seq[String]) => construct(args(0), args(1), args(2)))
  def of4[T <: Product :ClassManifest](construct : (String, String, String, String) => T) = apply((args : Seq[String]) => construct(args(0), args(1), args(2), args(3)))
  def of5[T <: Product :ClassManifest](construct : (String, String, String, String, String) => T) = apply((args : Seq[String]) => construct(args(0), args(1), args(2), args(3), args(4)))
  def of6[T <: Product :ClassManifest](construct : (String, String, String, String, String, String) => T) = apply((args : Seq[String]) => construct(args(0), args(1), args(2), args(3), args(4), args(5)))
  def of7[T <: Product :ClassManifest](construct : (String, String, String, String, String, String, String) => T) = apply((args : Seq[String]) => construct(args(0), args(1), args(2), args(3), args(4), args(5), args(6)))
  def of8[T <: Product :ClassManifest](construct : (String, String, String, String, String, String, String, String) => T) = apply((args : Seq[String]) => construct(args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7)))

  def apply[T <: Product :ClassManifest]( construct : Seq[String] => T) : DynamoObject[T] = {
    lazy val names = extractFieldNames(classManifest[T])
    lazy val keyName = names(0)
    lazy val className = classManifest.erasure.getSimpleName


    new DynamoObject[T] {
      def fromDynamo(attributes: Map[String, AttributeValue]) = construct(names.map{ name => attributes.get(name).map(_.getS).getOrElse(null)})
      def toDynamo(t: T) = names.zipWithIndex.filter{ case(name,i) => t.productElement(i) != null }.map {case (name, i) => (name, new AttributeValue().withS(t.productElement(i).toString))}.toMap

      override def hashKey = (keyName, "S")
      override def rangeKey = None
      protected def table = className
    }
  }

  protected def extractFieldNames(classManifest: ClassManifest[_]): Array[String] = {
    val clazz = classManifest.erasure
    try {
      // copy methods have the form copy$default$N(), we need to sort them in order, but must account for the fact
      // that lexical sorting of ...8(), ...9(), ...10() is not correct, so we extract N and sort by N.toInt
      val copyDefaultMethods = clazz.getMethods.filter(_.getName.startsWith("copy$default$")).sortBy(
        _.getName.drop("copy$default$".length).takeWhile(_ != '(').toInt)
      val fields = clazz.getDeclaredFields.filterNot(_.getName.startsWith("$"))
      if (copyDefaultMethods.length != fields.length)
        sys.error("Case class " + clazz.getName + " declares additional fields")
      if (fields.zip(copyDefaultMethods).exists { case (f, m) => f.getType != m.getReturnType })
        sys.error("Cannot determine field order of case class " + clazz.getName)
      fields.map(_.getName)
    } catch {
      case ex : Throwable => throw new RuntimeException("Cannot automatically determine case class field names and order " +
        "for '" + clazz.getName + "', please use the 'jsonFormat' overload with explicit field name specification", ex)
    }
  }

  def asAttribute(v: Any, keyType: ScalarAttributeType): AttributeValue = asAttribute(v, keyType.toString)

  def asAttribute(v: Any, keyType: String): AttributeValue = keyType match {
    case "S" => new AttributeValue().withS(v.toString)
    case "N" => new AttributeValue().withN(v.toString)
    case aType => sys.error("Not supported attribute type [%s]" format aType)
  }
}

object Demo extends App{
  case class Tst(id :String, name: String, email: String)
  implicit val ss = DynamoObject.of3(Tst)
  val tst = Tst("12312321", "Piotr", "piotrga@gmail.com")
  assert(ss.fromDynamo(ss.toDynamo(tst)) == tst)


  val tst2 = Tst("12312321", "Piotr", null)
  assert(ss.fromDynamo(ss.toDynamo(tst2)) == tst2)


}

