package com.zeebox.dynamo

import com.amazonaws.services.dynamodb.model.{KeySchemaElement, AttributeValue}




trait DynamoObject[T]{
  protected implicit def toAttribute(value : String) = new AttributeValue().withS(value)
  protected def key(attrName:String, attrType: String) = new KeySchemaElement().withAttributeName(attrName).withAttributeType(attrType)

  protected def table : String
  def toDynamo(t:T) : Map[String, AttributeValue]
  def fromDynamo(attributes: Map[String, AttributeValue]) : T
  def table(prefix: String): String = prefix + table
  def key: KeySchemaElement = key("id", "S")
  def range: Option[KeySchemaElement] = None
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
      def fromDynamo(attributes: Map[String, AttributeValue]) = construct(names.zipWithIndex.map{ case (name, i) => attributes.get(name).map(_.getS).getOrElse(null)})
      def toDynamo(t: T) = names.zipWithIndex.filter{ case(name,i) => t.productElement(i) != null }.map {case (name, i) => (name, new AttributeValue().withS(t.productElement(i).toString))}.toMap

      override def key = key(keyName, "S")
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
      case ex => throw new RuntimeException("Cannot automatically determine case class field names and order " +
        "for '" + clazz.getName + "', please use the 'jsonFormat' overload with explicit field name specification", ex)
    }
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

