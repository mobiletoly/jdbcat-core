JDBCat - JDBC Advanced Templates `JDBC=^.^=`

## Overview

JDBCat is a small layer on a top of JDBC. As of now it does two things:
 
- Provides a transformation from JDBC types to Kotlin types and vice versa to ensure type safety.
- Privides a wrapper around JDBC transactions to add support for coroutines.

While many frameworks provide some sort of database-independent solutions, we believe that in many cases
it is not always necessary and sometimes you want to go beyond a "most common denominator". For example,
if you know that database for your project is going to be PostgreSQL - why not to enjoy all benefits that
PostgreSQL provides using SQL?

## Show me an example

We understand that reading a long text can be time consuming. If you are the one who likes to go straight to a code,
here is an example for you:

[Very Simple Example](https://github.com/mobiletoly/jdbcat/blob/master/jdbcat-examples/src/main/kotlin/jdbcat/examples/pgexample.kt)

or

[Ktor-based Web Service](https://github.com/mobiletoly/jdbcat-ktor)

If you are brave enough to keep reading - we are going to show you a very basic code that includes:

- Create table layouts to represent SQL tables in database
- Create data models to read from or write to SQL tables
- Performs a basic initialization of tables with some initial data
- Query table records

### Table layout

In this tutorial we will create and initialize two tables - *departments* and *employees* and we will demonstrate
some basic query capabilities.

In order for JDBCat to provide type safety - you must declare table layout with column types that later will
be mapped to corresponding Kotlin types. JDBCat uses singletons (Kotlin's `object`) to declare table layout.
While most of the column types and SQL constructions in this tutorial are database-independent, but some are not.
In this tutorial we decided to use *PostgreSQL* database and we are going to use PostgreSQL
[SERIAL](https://www.postgresql.org/docs/10/static/datatype-numeric.html#DATATYPE-SERIAL)
type to generate unique auto-incremented primary keys. 

Let's declare first table *departments* represented by `Departments` object:

```kotlin
object Departments : Table(tableName = "departments") {
    val code = varchar("code", size = 3, specifier = "PRIMARY KEY").nonnull()
    val name = varchar("name", size = 100, specifier = "UNIQUE").nonnull()
    val countryCode = varchar("country_code", size = 3).nonnull()
    val city = varchar("city", size = 20).nonnull()
    val comments = pgText("comments")
    val dateCreated = javaDate("date_created").nonnull()
}
```

No surprise that table layout has type `Table` and actual table name is specified by `tableName` constructor
parameter. Next line declares field `code` that result in generating column with SQL type
`VARCHAR(3) PRIMARY KEY NOT NULL`. Next line declares column `name VARCHAR(100) UNIQUE` etc.
Internally `varchar(...).nullable()` function returns `VarCharColumn` type that knows 
casting from SQL VARCHAR type to Kotlin's `String`. If you decide to remove `.nullable()` call then instead
`NullableVarCharColumn` will be used to convert to/from Kotlin's `String?` type. 

Also worth to mention that JDBCat does not validate `specifier` parameter or does not treat is in any specific
way, it is just a convenient parameter to append some text after column type. You can put any database-specific
statements there if that is what you need.

`pgText` is an extension method that uses PostgreSQL `TEXT` type. We are planning to supply JDBCat with variety 
of extension methods for different databases. Since we have not specified `.nonnull()` here, 

`javaDate` is an extension method as well, but it is not database specific. Instead it is declared as
BIGINT NOT NULL type, stores timestamp in milliseconds since EPOCH and knows how to convert to and from 
`java.util.Date` type. 

Now let's declare *employees* table represented by `Employees` object:

```kotlin
enum class Gender {
    FEMALE,
    MALE
}

object Employees : Table(tableName = "employees") {
    val id = pgSerial("id", specifier = "PRIMARY KEY")
    val firstName = varchar("first_name", size = 50).nonnull()
    val lastName = varchar("last_name", size = 50).nonnull()
    val age = integer("age").nonnull()
    val gender = enumByName<Gender>("gender", size = 30).nonnull()
    val departmentCode = varchar(
        "department_code",
        size = 3,
        specifier = "REFERENCES ${Departments.tableName} (${Departments.code.name})"
    ).nonnull()
    val comments = pgText("comments")
    val dateCreated = javaDate("date_created").nonnull()
}
```

Here you can see foreign key declared via "REFERENCES departments (code)" SQL statement. That's right
`${Departments.tableName}` gets translated to "departments" table name and `${Departments.code.name}` gets
translated to "code" column name. This is a PostgreSQL flavor of creating foreign keys.

### Data models

So we know how to represent table, but how do we represent a specific row? Our suggestion to data classes
to represent a specific row. For example here is data class representing row in `Employees` table:

```kotlin
data class Employee(
    val id: Int? = null,
    val firstName: String,
    val lastName: String,
    val age: Int,
    val gender: Gender,
    val departmentCode: String,
    val comments: String?,
    val dateCreated: Date?
)
```

Later we will show how to serialize/deserialize data to/from table, but for now
let's review class itself. Everything is looking pretty straightforward, other than why have we declared `id`
field as `Int?` if we don't expect this column in table to be null (it is auto-incremented value). The answer is
because if we want to create this object to insert it into a database, we don't know what value we have to assign
to `id`, this is a job that database has to perform in case with auto-incremented (or otherwise generated) column.
So we just leave it null and let DB take care of it. Once we read this row back from database - `id` field will
be populated with a valid data.

To represent row in `Departments` table, we can declare this data class:
```kotlin
data class Department(
    val code: String,
    val name: String,
    val countryCode: String,
    val city: String,
    val comments: String? = null,
    val dateCreated: Date = Date()
)
```
This time `code` is not null, because we always assigning it during creation of new Department object.

### Create and initialize tables

JDBC uses `DataSource` interface to supply `Connection` object to represent database connections. In our example
we are using awesome HikaryCP JDBC connection pool, but for simplicity we are not going to show how initialize it,
please refer to our 
[Very Simple Example](https://github.com/mobiletoly/jdbcat/blob/master/jdbcat-examples/src/main/kotlin/jdbcat/examples/pgexample.kt)
to take a look. All we know is that somehow we have received `dataSource` variable that represents connection pool.

We always want to put our code in JDBCat's transaction block. It can be done by doing:

```kotlin
dataSource.tx { connection -> /* Use connection object to perform JDBC operations */ }
```

`.tx {...}` is an extension function over standard `DataSource` interface over JDBC. It starts a transaction,
executes block of code inside `{...}` and performs a commit (or rollback in case of exception). What it also does
is it creates (or reuses) connection acquired from JDBC connection pool. `.tx` function was implemented in a way
that it knows how to run inside Kotlin's coroutines. It makes sure to save and restore Connection objects even
when running multiple coroutines on multiple threads. By default it uses `Dispatchers.IO` dispatcher (which is
recommended for blocking I/O operation such as JDBC calls), but there is override of `.tx()` function that allows
you to specify different dispatcher. Keep in mind that you can have any level of nested `.tx {...}` blocks,
but transaction will be committed only when top-most `.tx` block is finished. This approach allows you to build
your functionality where your function can start a transaction without concerns that caller called your function
inside of transaction. And if it happened that caller actually called your function from its own transaction -
then your function will reuse already existing transaction. Pretty cool, right? 

Let's get back to our example. Assuming, that we always want to have a clean state, let's make sure that we
drop tables if they exist:

```kotlin
dataSource.tx { connection ->
    connection.createStatement().executeUpdate("DROP TABLE IF EXISTS ${Employees.tableName}")
    connection.createStatement().executeUpdate("DROP TABLE IF EXISTS ${Departments.tableName}")
    
    createDepartmentsTable()
    createEmployeesTable()
    insertInitialDataToDepartmentsTable()
    addInitialDataToEmployeesTable()
    // ...
}
```

**If you have not opened Very Simple Example yet - it is time to do it now**. What are we going to show you is how to
create precompiled templates that you can use to generate JDBC's `PreparedStatement` interface.
Let's take a look at `createEmployeesTable()` function.

```kotlin
private suspend fun createEmployeesTable() = dataSource.txRequired { connection ->
    val createEmployeesTemplate: SqlTemplate = sqlTemplate(Employees) {
        """
        | CREATE TABLE IF NOT EXISTS $tableName (${columns.sqlDefinitions});
        | CREATE INDEX ${age.sqlIndexName} ON $tableName ( $age );
        """
    }
    val createEmployeesStmt: TemplatizedStatement = createEmployeesTemplate.prepareStatement(connection)
    createEmployeesStmt.executeUpdate()
}
```

`TemplatizedStatement` is a delegate of `PreparedStatement` interface, so you have access to all `PreparedStatement`
methods.

Let's quicking take a look at what is going on here. `suspend` is required because all JDBCat transaction
functions `d.tx()` are suspend function, since we know that JDBC is a long-running operation. `.txRequired()` is
another form of transaction. It is a slightly lighter version of `.tx()` where we actually say that it is MANDATORY
to call this function inside transaction, so it is a duty of caller to create a transaction. Sometimes it is good
to ensure that caller started a transaction to eliminate a chance that caller will be calling us like a regular
function instead of caller combining multiple calls into 1 transaction, because we might end up with multiple
transactions, instead of one (this is good for your own code when you really know what is going on and who is calling
you).

`sqlTemplate()` is a helper function that does pretty much nothing but (in our case) pass `Employees` singleton
object as `this` into a lambda. It simplifies some operations e.g. you don't need to type `${Employees.tableName}`
but you can type `$tableName` instead. Also it performs `trimMargin()` on a string that lambda returns. Still we
would suggest to use `sqlTemplate` for SQL templates because this function might end up doing more useful things
in a future. Also `sqlTemplate` may accept multiple tables as a parameter, in this case instead of `this` these
tables will be passed as lambda parameters that you can use to simplify typing as well, e.g.
```kotlin
sqlTemplate(Employees, Departments) { e, d ->
   // build SQL template using form such as ${e.tableName} ...
}
```

`createDepartmentsTemplate.prepareStatement(connection)` creates `TemplatizedStatement`
(delegate of `PreparedStatement`) object using connection object that was passed to us inside a transaction context.
And if you are wondering what `${columns.sqlDefinitions}` does - it actually creates a comma separated list of columns
with its definitions, e.g. `id SERIAL NOT NULL, first_name VARCHAR(50) NOT NULL, ...`
If you are wondering what `${age.sqlIndexName}` does, it creates a unique index name. Obviously you can use
your own index name here, but sometimes it is handy to let JDBCat to generate name "idx_employees_age" for you.
After you call JDBC `createEmployeesStmt.executeUpdate()` - your table should be created.

Now let's take a look at `addInitialDataToEmployeesTable()` call that inserts some initial employee records
into Employees table.

```kotlin
private suspend fun addInitialDataToEmployeesTable() = dataSource.txRequired { connection ->
    // ...
    val insertEmployeeTemplate = sqlTemplate(Employees) {
        """
        | INSERT INTO $tableName ($firstName, $lastName, $age, $gender, $departmentCode, $dateCreated, $comments)
        |   VALUES (${firstName.v}, ${lastName.v}, ${age.v}, ${gender.v}, ${departmentCode.v}, ${dateCreated.v}, ${comments.v})
        """
    }
    val insertEmployeeStmt = insertEmployeeTemplate.prepareStatement(
        connection = connection,
        returningColumnsOnUpdate = listOf(Employees.id)
    )
    val employees = employeesToAdd.map { employee ->
        insertEmployeeStmt.setColumns {
            it[Employees.firstName] = employee.firstName
            it[Employees.lastName] = employee.lastName
            it[Employees.age] = employee.age
            it[Employees.gender] = employee.gender
            it[Employees.departmentCode] = employee.departmentCode
            it[Employees.dateCreated] = employee.dateCreated!!
            it[Employees.comments] = employee.comments
        }
        insertEmployeeStmt.executeUpdate()
        val id: Int = insertEmployeeStmt.generatedKeys.singleRow {
            it[Employees.id]
        }
        employee.copy(id = id)
    }
}
```

Let's review `insertEmployeeTemplate` first. You already familiar with `sqlTemplate()` function, e.g. you
know that `$firstName` will be substituted with a table's column name `first_name`. So the only
new thing here is `.v` notation. Something like `${firstName.v}` says that later Kotlin value (String, Int etc)
will be passed here, so be ready. Here we enumerate all fields that we want to handle.
There is a shorter form for this:

```
INSERT INTO $tableName (${columns.sqlNames})
    VALUES (${columns.sqlValues})
```
but the problem is that we want to insert all columns but `id`, because `id` is auto-generated column. But it still
can be simplified based on a fact that `$columns` is a collection and you can use Kotlin's collection helpers
to remove value from collection. So the end result could be:
```
INSERT INTO $tableName (${(columns - id).sqlNames})
    VALUES (${(columns - id).sqlValues})
```

So how do we actually generate JDBC statement from this tempalate? We are going to use the same `.prepareStatement`
method that we used before, but this time with additional parameters. 
```kotlin
val insertEmployeeStmt: TemplatizedStatement = insertEmployeeTemplate.prepareStatement(
    connection = connection,
    returningColumnsOnUpdate = listOf(Employees.id)
)
```

we still pass a Connection object, but this time we are also asking JDBC to return us autogenerated `id` column.
It is not required, but let's pretend that we really need that `id` column. So the last part is how to pass
actual data to that templatized prepared statement. This is done via `setColumns()` call:

```
insertEmployeeStmt.setColumns {
    it[Employees.firstName] = employee.firstName
    it[Employees.lastName] = employee.lastName
    // ...
```

`setColumns` passes `ColumnValueBuilder` object as `it` reference to lambda and data can be initialized
as `it[column] = value`. Not only it is convenient, but this assignment enforces type safety as well
(e.g. you cannot assign Int value to column created with `varchar` function in table layout).
Then we call `stmt.executeUpdate()` to let JDBC to do its work.

There is another interesting call:
```kotlin
val id: Int = stmt.generatedKeys.singleRow {
    it[Employees.id]
}
employee.copy(id = id)
```

since we have requested JDBC to return us generated Employees.id - that is exactly how we get it back. JDBC
returns generated columns as (in our case) first row in ResultSet. We get it back with type safetly that Kotlin
provides (Int in our case, e.g. would get a compilation error if you'd try to assign it to string).
Last step is simply return a copy of Employee object with new id field (typical "data class" construction).

### Query data

We are going to show an example of querying data from both Departments and Employees table in one statement. Our goal
is to query all records for employees that are in between 35 and 45 years old and are working in US offices.

```kotlin
val selectByAgeAndCountryTemplate = sqlTemplate(
    Departments, Employees
) { d: Departments, e: Employees ->
    """
    | SELECT t_dep.*, t_emp.*
    |   FROM
    |       ${d.tableName} AS t_dep
    |       LEFT OUTER JOIN ${e.tableName} AS t_emp
    |           ON t_dep.${d.code} = t_emp.${e.departmentCode}
    |   WHERE
    |       t_dep.${d.countryCode} = ${d.countryCode.v}
    |           AND
    |       t_emp.${e.age} >= ${e.age["lowerAge"]}
    |           AND
    |       t_emp.${e.age} <= ${e.age["upperAge"]}
    |   ORDER BY
    |       t_dep.${d.code}, t_emp.${e.lastName}, t_emp.${e.firstName}
    """
}
val selectByAgeAndCountryStmt = selectByAgeAndCountryTemplate.prepareStatement(
    connection
).setColumns {
    it[Departments.countryCode] = "USA"
    it[Employees.age, "lowerAge"] = 35
    it[Employees.age, "upperAge"] = 45
}
// ...
```

It is very similar to what we did before when were inserting records in a database. With few new features.
First of all as you can see - we are passing multiple tables (`Departments` and `Employees`) to `sqlTemplate()`
and we are going to get them back as lambda arguments, so we could assign shorter variable names.
After that we provide SQL query. And here is something new. While something like `${d.countryCode.v}` might look
already familiar and it actually tells that we are going to provide an actual value via `it[Departments.countryCode]`
assignment later, but we also specify named parameters for `age`. This is convenient if you still want to keep
type safety for value assignments for the same column.

### Let's print results

Last but not least, is how to get a result from execution of query:

```kotlin
val result = selectByAgeAndCountryStmt.executeQuery().asSequence().map {
    val employee = Employee.from(it)
    val department = Department.from(it)
    employee to department
}.toList()
```

In this construction `it` is an instance of `ColumnValueExtractor` object. Here is an example of `from()`
function defined for Employee class:

```kotlin
data class Employee(
    val id: Int? = null,
    val firstName: String,
    val lastName: String,
    val age: Int,
    val gender: Gender,
    val departmentCode: String,
    val comments: String?,
    val dateCreated: Date?
) {
    companion object {
        fun from(value: ColumnValueExtractor) = Employee(
            id = value[Employees.id],
            firstName = value[Employees.firstName],
            lastName = value[Employees.lastName],
            age = value[Employees.age],
            gender = value[Employees.gender],
            departmentCode = value[Employees.departmentCode],
            comments = value[Employees.comments],
            dateCreated = value[Employees.dateCreated]
        )
    }
}
```

Again, everything is typesafe. Enjoy! 

Below is some additional information for advanced usage.

## How to use aggregate functions?

We do have a class `EphemeralTable` that allows to deal with aliases. So here is an example of how we can calculate
all Employees in Seattle's office of our company.

```kotlin
object CounterResult : EphemeralTable() {
    val counter = integer("counter").nonnull()
}

fun run() = runBlocking {
    // ...
    val countAllEmployeesTemplate = sqlTemplate(Employees, CounterResult) { e, cr ->
        "SELECT COUNT(*) AS ${cr.counter} FROM ${e.tableName} WHERE ${e.departmentCode} = ${e.departmentCode.v}"
    }
    val countAllEmployeesStmt = countAllEmployeesTemplate.prepareStatement(connection).setColumns {
        it[Employees.departmentCode] = "SEA"
    }
    val numberOfEmployees = countAllEmployeesStmt.executeQuery().singleRow {
        it[CounterResult.counter]
    }
}
```

## Custom column types

It is easy to add a custom column types for generic SQL or for specific database dialect. For example here is
how we have implemented SERIAL type for PostgreSQL

```kotlin
fun Table.pgSerial(name: String, specifier: String? = null) =
    registerColumn(object : Column<Int>(name = name, type = "SERIAL", specifier = specifier, table = this) {
        override fun getData(rs: ResultSet, paramIndex: Int): Int {
            return rs.getInt(paramIndex)
        }
        override fun setData(stmt: PreparedStatement, paramIndex: Int, value: Any?) {
            stmt.setInt(paramIndex, value as Int)
        }
    })
```

since we always need to register column - we always have to wrap custom column in `registerColumn` block.
Postgres SERIAL type in 4 bytes, so we declare it as Column<Int>. We specify native SQL type "SERIAL" in
`type` parameter of Column. Then last step we want to do is to specify getData() method to deserialize data
from JDBC's `ResultSet` object. We know that SERIAL can never be null, so we don't have any special null handling,
we just read and write integer objects.

Here is a little bit more complicated example of PostgreSQL's TEXT type. It is more complicated because it can
contain null values.

First we declare nullable version of TEXT column (by default SQL columns are NULL):

```kotlin
class PgNullableTextColumn constructor(
    name: String,
    specifier: String? = null,
    table: Table
) : Column<String?>(name = name, type = "TEXT", specifier = specifier, table = table) {

    override fun getData(rs: ResultSet, paramIndex: Int): String? = rs.getString(paramIndex)

    override fun setData(stmt: PreparedStatement, paramIndex: Int, value: Any?) {
        if (value == null) {
            stmt.setNull(paramIndex, Types.VARCHAR)
        } else {
            stmt.setString(paramIndex, value as String)
        }
    }

    fun nonnull() = table.unregisterColumn(this).registerColumn(
        PgTextColumn(name = name, specifier = specifier, table = table)
    )
}
```

Then we declare non-null version of TEXT column

```kotlin
class PgTextColumn constructor(
    name: String,
    specifier: String? = null,
    table: Table
) : Column<String>(name = name, type = "TEXT NOT NULL", specifier = specifier, table = table) {

    override fun setData(stmt: PreparedStatement, paramIndex: Int, value: Any?) {
        stmt.setString(paramIndex, value as String)
    }
    override fun getData(rs: ResultSet, paramIndex: Int) = rs.getString(paramIndex)!!
}
```

As you can see - nullable verion of TEXT can be converted to non-nullable by calling `nonnull()` method.

Last step - we declare a convenience (extension) method

```kotlin
fun Table.pgText(name: String, specifier: String? = null) = registerColumn(
    PgNullableTextColumn(name = name, specifier = specifier, table = this)
)
```

Using this pattern you can easily add other database specific methods. **Please contribute to this projects!**
