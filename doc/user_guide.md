Mapping object to dynamo
------------------------
In order to be able to do any operations in dynamo for type `T` you need create an instance of `DynamoObject[T]` and make it implicit in the scope of operations.

### Reflection based DynamoObject

If the object you want to persist in Dynamo is simple you can use the dynamically generated DynamoObject.

```scala
case class Account(id: String, balance: Double, lastModified: Date)
implicit val AccountDO = DynamoObject.of3(Account)
```

### Manually defining DynamoObject

If you need more control you can define the mapping yourself:

```scala
    case class Account(id: String, balance: Double, lastModified: Date)

    implicit val AccoundDO : DynamoObject[Account] = new DynamoObject[Account]{

        def table = "account"
        def keyName = "id"
        def keyType = "S"

        def toDynamo( a : Account) : Map[String, AttributeValue] =
            Map(    "id" -> toAttribute(a.id),
                    "balance" -> toAttribute(a.balance.toString),
                    "lastModified" -> toAttribute(formatter.toString(a.lastModified)
               )

        def fromDynamo(f: Map[String, AttributeValue]) : Account =
            Account(    f("id").getS,
                        f("balance").getS.toDouble,
                        formatter.parse(f("lastModified").getS)
                   )
    }
```

Blocking vs non-blocking
------------------------

Depending on your needs you can either use blocking or non-blocking version of operations by importing the appropriate package.

### Blocking
```scala

import blocking._
implicit val dynamo = ...
implicit val timeout = ...

Save(julian)
println(Read[Person](julian.id))
```

### Non-blocking

```scala

import nonblocking._
val dynamo = ...
implicit val timeout = ...

(Save(julian) executeOn dynamo)
    .flatMap ( saved =>  Read[Person](saved.id) executeOn dynamo )
    .onSuccess{ case p => println(p) }

```
If you dont like implicit variables:
```scala
Save(julian) executeOn(dynamo)(10 seconds)
```
### Monadic non-blocking
Dynamo operations are monadic so you can compose them at will:
```scala
import nonblocking._
val dynamo = ...
implicit val timeout = ...

def transfer(amount: Double, fromId: String, toId: String) = for{
    accountFrom <- Read[Account](fromId)
    accountTo <- Read[Account](toId)
    accountFromAfter <- Save(accountFrom.copy(balance = accountFrom - amount))
    accountToAfter <- Save(accountTo.copy(balance = accountTo + amount))
} yield (accountFromAfter, accountToAfter )

transfer(100, "account-123", "account-987") executeOn dynamo
```

### Implicits to make live easier
When the result type is known and `dynamo` and `timeout` are in scope we can benefit from even simpler syntax.
You need to `import com.zeebox.dynamo._` to activate them.
```scala
import com.zeebox.dynamo._
import nonblocking._

implicit val dynamo = ...
implicit val timeout = ...

def findById(id : String) : Future[Option[Person]] = Read[Person]("123")
// this will translate to Read[Person]("123") executeOn dynamo

def findByIdBlocking(id : String) : Option[Person] = Read[Person]("123")
// this will translate to Await.result(Read[Person]("123") executeOn dynamo, timeout)

```

Basic operations
----------------

### Saving objects
```scala
import com.zeebox.dynamo._

implicit val dynamo = ...
implicit val timeout = ...

val julian = Person("id-123", "Julian", "julian@gmail.com")

import nonblocking._
Save(julian) executeOn dynamo onSuccess { case p => println(p) }

//or

import blocking._
println(Save(julian))
```

### Reading objects
```scala
import com.zeebox.dynamo._

implicit val dynamo = ...
implicit val timeout = ...

import nonblocking._
Read[Person]("123") executeOn dynamo onSuccess { case p => println(p) }

//or

import blocking._
println(Read[Person]("123"))
```

### Deleting object
```scala
import nonblocking._
DeleteById[Person]("123") executeOn dynamo onSuccess { case _ => println("Deleted 123") }

//or

import blocking._
DeleteById[Person]("123")
```

Adding new operations
---------------------
The async-dynamo was written following open-closed principle. This means that you can add new operations easily and they will work with the library in the same way as the operations, which are pre-packaged with library.
For example if we wanted to add `ListAll` operation:
```scala
case class ListAll[T](limit : Int)(implicit dyn:DynamoObject[T]) extends DbOperation[Seq[T]]{
  def execute(db: AmazonDynamoDBClient, tablePrefix:String) : Seq[T] = {
    db.scan(new ScanRequest(dyn.table(tablePrefix)).withLimit(limit)).getItems.asScala.map {
      item => dyn.fromDynamo(item.asScala.toMap)
    }
  }
}

```

Admin operations
----------------
### Creating table
### Checking table status
### Deleting table