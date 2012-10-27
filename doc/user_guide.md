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

Basic operations
----------------
### Blocking vs non-blocking
Depending on your needs you can either use blocking or non-blocking version of operations by importing the appropriate package.

_Blocking:_
```scala

import blocking._
implicit val dynamo = ...
implicit val timeout = ...

Save(julian)
println(Read[Person](julian.id))
```
_Non-blocking_

```scala

import nonblocking._
val dynamo = ...
implicit val timeout = ...

(Save(julian) executeOn dynamo)
    .flatMap ( saved =>  Read[Person](saved.id) executeOn dynamo )
    .onResult ( println(_) )

```
_Monadic non-blocking_
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
_Implicits to make live easier_
When the result type is known and `dynamo` and `timeout` are in scope we can benefit from even simpler syntax
```scala
implicit val dynamo = ...
implicit val timeout = ...

def findById(id : String) : Future[Option[Person]] = Read[Person]("123") // this will translate to Read[Person]("123") executeOn dynamo
def findByIdBlocking(id : String) : Option[Person] = Read[Person]("123") // this will translate to Await.result(Read[Person]("123") executeOn dynamo, timeout).asInstanceOf[Person]

```

### Saving objects
```scala
import com.zeebox.dynamo._
import nonblocking._
val julian = Person("id-123", "Julian", "julian@gmail.com")
val savedFuture = Save(julian) executeOn dynamo
val saved = Await.result(savedFuture, 1 second)
```

### Reading objects
TBD

### Deleting object
TBD

Adding new operations
---------------------
The async-dynamo was written following open-closed principle. This means that you can add new operations easily and they will work with the library in the same way as the operations, which are pre-packaged with library.
