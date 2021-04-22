package jdbcat.examples

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import jdbcat.core.ColumnValueExtractor
import jdbcat.core.EphemeralTable
import jdbcat.core.Table
import jdbcat.core.asSequence
import jdbcat.core.enumByName
import jdbcat.core.integer
import jdbcat.core.singleRow
import jdbcat.core.sqlDefinitions
import jdbcat.core.sqlNames
import jdbcat.core.sqlTemplate
import jdbcat.core.sqlValues
import jdbcat.core.tx
import jdbcat.core.txRequired
import jdbcat.core.varchar
import jdbcat.dialects.pg.pgSerial
import jdbcat.dialects.pg.pgText
import jdbcat.ext.javaDate
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking
import java.util.Date
import java.util.Properties

fun main() {
    PgExample().run()
}

class PgExample {

    private val hikariProps = Properties().apply {
        setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource")
        setProperty("dataSource.url", "jdbc:postgresql://localhost:5432/postgres")
        setProperty("dataSource.user", "postgres")
        setProperty("dataSource.password", "postgresspass")
    }
    private val hikariConfig = HikariConfig(hikariProps).apply {
        isAutoCommit = false
    }
    private val dataSource = HikariDataSource(hikariConfig)

    fun run() = runBlocking {

        val report = dataSource.tx { connection ->
            // Drop tables if they are already exist
            connection.createStatement().executeUpdate("DROP TABLE IF EXISTS ${Employees.tableName}")
            connection.createStatement().executeUpdate("DROP TABLE IF EXISTS ${Departments.tableName}")

            createDepartmentsTable()
            createEmployeesTable()
            insertInitialDataToDepartmentsTable()
            addInitialDataToEmployeesTable()

            val countAllEmployeesTemplate = sqlTemplate(Employees, CounterResult) { e, cr ->
                "SELECT COUNT(*) AS ${cr.counter} FROM ${e.tableName} WHERE ${e.departmentCode} = ${e.departmentCode.v}"
            }
            val countAllEmployeesStmt = countAllEmployeesTemplate.prepareStatement(connection).setColumns {
                it[Employees.departmentCode] = "SEA"
            }
            println("* Count all Employee record:\n $countAllEmployeesStmt")
            val counter = countAllEmployeesStmt.executeQuery().singleRow {
                it[CounterResult.counter]
            }
            println("--- Number of Employees: $counter\n")

            // Perform query on multiple tables
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
            println("* Select by age and country SQL:\n $selectByAgeAndCountryStmt")

            selectByAgeAndCountryStmt.executeQuery().asSequence().map {
                val employee = Employee.from(it)
                val department = Department.from(it)
                employee to department
            }.toList()
        }

        println("\n--- List of USA employees with age in between 35 and 45 ---")
        report.forEach {
            println("+ ${it.first}")
            println("+---- ${it.second}")
        }
    }

    private suspend fun createDepartmentsTable() = dataSource.txRequired { connection ->
        // Create Departments table
        val createDepartmentsTemplate = sqlTemplate(Departments) {
            """
            | CREATE TABLE IF NOT EXISTS $tableName (
            |   ${columns.sqlDefinitions},
            |   UNIQUE ( $countryCode, $city )
            | )
            """
        }
        val createDepartmentStmt = createDepartmentsTemplate.prepareStatement(connection)
        println("* Create Departments Table SQL: $createDepartmentStmt\n")
        createDepartmentStmt.executeUpdate()
    }

    private suspend fun createEmployeesTable() = dataSource.txRequired { connection ->
        // Create Employees table
        val createEmployeesTemplate = sqlTemplate(Employees) {
            """
            | CREATE TABLE IF NOT EXISTS $tableName (${columns.sqlDefinitions});
            | CREATE INDEX ${age.sqlIndexName} ON $tableName ( $age );
            """
        }
        val createEmployeesStmt = createEmployeesTemplate.prepareStatement(connection)
        println("* Create Employees Table SQL: ${createEmployeesTemplate.sql}\n")
        createEmployeesStmt.executeUpdate()
    }

    private suspend fun insertInitialDataToDepartmentsTable() = dataSource.txRequired { connection ->
        // Insert initial data into Departments table
        val departmentsToAdd = listOf(
            Department(code = "SEA", name = "Seattle's Office", countryCode = "USA", city = "Seattle",
                comments = "Headquarter and R&D", dateCreated = Date(Date().time - 99999999999L)),
            Department(code = "CHI", name = "Chicago's Office", countryCode = "USA", city = "Chicago",
                comments = "Financial departmentCode", dateCreated = Date(Date().time - 77777777777L)),
            Department(code = "BER", name = "Berlin's Office", countryCode = "DEU", city = "Berlin",
                comments = "R&D", dateCreated = Date(Date().time - 55555555555L)),
            Department(code = "AMS", name = "Amsterdam's Office", countryCode = "NLD", city = "Amsterdam",
                comments = "Just for fun :)", dateCreated = Date(Date().time - 33333333333L))
        )
        // Create SQL template to INSERT data into table.
        // "columns" property represent list of all columns that this table has. So basically ${columns.sqlNames}
        // will be expanded into a list of column names "code, name, country_code, ...", while ${columns.sqlValues}
        // will be expanded in a list of special markers that later will be substituted with a real values.
        // Normally if you expect for this method to be called  mutliple times - it is better to move this
        // declaration outside of method (e.g. to companion object).
        val insertDepartmentTemplate = sqlTemplate(Departments) {
            "INSERT INTO $tableName (${columns.sqlNames}) VALUES (${columns.sqlValues})"
        }
        val insertDepartmentStmt = insertDepartmentTemplate.prepareStatement(connection)

        val departments = departmentsToAdd.map { department ->
            insertDepartmentStmt.setColumns {
                // Let's copy real values into SQL template by substituting value markers with actual values.
                it[Departments.code] = department.code
                it[Departments.name] = department.name
                it[Departments.countryCode] = department.countryCode
                it[Departments.city] = department.city
                it[Departments.dateCreated] = department.dateCreated
                it[Departments.comments] = department.comments
            }
            println("* Add Department SQL: $insertDepartmentStmt")
            insertDepartmentStmt.executeUpdate()
            department // return self (with autogenerated fields you will return modified Department object)
        }
        println("\n--- List of added departments ---")
        for (department in departments) {
            println("- Added department: $department")
        }
        println()
    }

    // Add initial data into Employees table
    private suspend fun addInitialDataToEmployeesTable() = dataSource.txRequired { connection ->
        val employeesToAdd = listOf(
            Employee(firstName = "Toly", lastName = "Pochkin", age = 40, gender = Gender.MALE,
                departmentCode = "SEA", comments = "CEO", dateCreated = Date(Date().time - 89999999999L)),
            Employee(firstName = "Jemmy", lastName = "Hyland", age = 27, gender = Gender.MALE,
                departmentCode = "SEA", comments = "CPO", dateCreated = Date(Date().time - 79999999999L)),
            Employee(firstName = "Doreen", lastName = "Fosse", age = 35, gender = Gender.FEMALE,
                departmentCode = "CHI", comments = "CFO", dateCreated = Date(Date().time - 69999999999L)),
            Employee(firstName = "Brandy", lastName = "Ashworth", age = 39, gender = Gender.FEMALE,
                departmentCode = "BER", comments = "Lead engineer", dateCreated = Date(Date().time - 45555555555L)),
            Employee(firstName = "Lenny", lastName = "Matthews", age = 50, gender = Gender.MALE,
                departmentCode = "AMS", comments = "DJ", dateCreated = Date(Date().time - 25555555555L))
        )

        // Create SQL template to INSERT data into table. As you can see - it is different from what we saw in
        // insertInitialDataToDepartmentsTable() method. Since "id" is a primary auto-generated key,
        // we specify all fields names and values without "id" field.
        // So for examplke $firsName will be expanded to "first_name" column name, while ${firstName.v} will
        // be expanded into a special firstName value marker that later will be substituted with actual value.
        val insertEmployeeTemplate = sqlTemplate(Employees) {
            """
            | INSERT INTO $tableName (
            |   $firstName, $lastName, $age, $gender, $departmentCode, $dateCreated, $comments
            | ) VALUES (
            |   ${firstName.v}, ${lastName.v}, ${age.v}, ${gender.v}, ${departmentCode.v}, ${dateCreated.v}, ${comments.v}
            | )
            """
        }
        /*
        TIP: you could use Kotlin collections functionality to specify all table columns other than "id" columns.
        You could rewrite a construction above using a collection with removed "id" field:
            val insertEmployeeTemplate = sqlTemplate(Employees) {
                """
                | INSERT INTO $tableName (${(columns - id).sqlNames})
                |   VALUES (${(columns - id).sqlValues})
                """
            }
        */

        val insertEmployeeStmt = insertEmployeeTemplate.prepareStatement(
            connection = connection,
            returningColumnsOnUpdate = listOf(Employees.id)
        )

        val employees = employeesToAdd.map { employee ->
            insertEmployeeStmt.setColumns {
                // Let's copy real values into SQL template by substituting value markers with actual values.
                it[Employees.firstName] = employee.firstName
                it[Employees.lastName] = employee.lastName
                it[Employees.age] = employee.age
                it[Employees.gender] = employee.gender
                it[Employees.departmentCode] = employee.departmentCode
                it[Employees.dateCreated] = employee.dateCreated!!
                it[Employees.comments] = employee.comments
            }
            println("* Add Employee SQL: $insertEmployeeStmt")
            insertEmployeeStmt.executeUpdate()
            val id: Int = insertEmployeeStmt.generatedKeys.singleRow {
                it[Employees.id]
            }
            employee.copy(id = id)
        }
        println("\n--- List of added employees ---")
        for (employee in employees) {
            println("- Added employee: $employee")
        }
        println()
    }
}

// "Count all employees" result.
object CounterResult : EphemeralTable() {
    val counter = integer("counter").nonnull()
}

// -------------------------
// Table definitions
// -------------------------

enum class Gender {
    FEMALE,
    MALE
}

// Table definition (representation of "departments" database table)
object Departments : Table(tableName = "departments") {
    val code = varchar("code", size = 3, specifier = "PRIMARY KEY").nonnull()
    val name = varchar("name", size = 100, specifier = "UNIQUE").nonnull()
    val countryCode = varchar("country_code", size = 3).nonnull()
    val city = varchar("city", size = 20).nonnull()
    val comments = pgText("comments")
    val dateCreated = javaDate("date_created").nonnull()
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

// -------------------------
// Rows definitions
// -------------------------

data class Department(
    val code: String,
    val name: String,
    val countryCode: String,
    val city: String,
    val comments: String? = null,
    val dateCreated: Date = Date()
) {
    companion object {
        fun from(value: ColumnValueExtractor) = Department(
            code = value[Departments.code],
            name = value[Departments.name],
            countryCode = value[Departments.countryCode],
            city = value[Departments.city],
            comments = value[Departments.comments],
            dateCreated = value[Departments.dateCreated]
        )
    }
}

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
