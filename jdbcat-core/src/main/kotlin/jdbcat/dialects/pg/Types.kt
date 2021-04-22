package jdbcat.dialects.pg

import jdbcat.core.Column
import jdbcat.core.Table
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

/**
 * Create SERIAL column
 * https://www.postgresql.org/docs/10/static/datatype-numeric.html#DATATYPE-SERIAL
 */
fun Table.pgSerial(name: String, specifier: String? = null) =
    registerColumn(object : Column<Int>(name = name, type = "SERIAL", specifier = specifier, table = this) {
        override fun getData(rs: ResultSet, paramIndex: Int): Int {
            return rs.getInt(paramIndex)
        }
        override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
            statement.setInt(paramIndex, value as Int)
        }
    })

/**
 * Create BIGSERIAL column
 * https://www.postgresql.org/docs/10/static/datatype-numeric.html#DATATYPE-SERIAL
 */
fun Table.pgBigSerial(name: String, specifier: String? = null) =
    registerColumn(object : Column<Long>(name = name, type = "BIGSERIAL", specifier = specifier, table = this) {
        override fun getData(rs: ResultSet, paramIndex: Int): Long {
            return rs.getLong(paramIndex)
        }
        override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
            if (value == null) {
                statement.setNull(paramIndex, Types.BIGINT)
            } else {
                statement.setLong(paramIndex, value as Long)
            }
        }
    })

/**
 * Create TEXT column
 * https://www.postgresql.org/docs/10/static/datatype-character.html
 */
fun Table.pgText(name: String, specifier: String? = null) = registerColumn(
    PgNullableTextColumn(name = name, specifier = specifier, table = this)
)

// ---------------------------------------------------------------------------------------------------------------------

class PgNullableTextColumn constructor(
    name: String,
    specifier: String? = null,
    table: Table
) : Column<String?>(name = name, type = "TEXT", specifier = specifier, table = table) {

    override fun getData(rs: ResultSet, paramIndex: Int): String? = rs.getString(paramIndex)

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        if (value == null) {
            statement.setNull(paramIndex, Types.VARCHAR)
        } else {
            statement.setString(paramIndex, value as String)
        }
    }

    fun nonnull() = table.unregisterColumn(this).registerColumn(
        PgTextColumn(name = name, specifier = specifier, table = table)
    )
}

class PgTextColumn constructor(
    name: String,
    specifier: String? = null,
    table: Table
) : Column<String>(name = name, type = "TEXT NOT NULL", specifier = specifier, table = table) {

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        statement.setString(paramIndex, value as String)
    }
    override fun getData(rs: ResultSet, paramIndex: Int) = rs.getString(paramIndex)!!
}

// ---------------------------------------------------------------------------------------------------------------------

/**
 * Helper function that iterates through columns and generates string like
 * column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, ...
 *
 * This is to support Postgres upsert functionality:
 * https://www.postgresql.org/docs/9.5/static/sql-insert.html
 */
val Collection<Column<*>>.pgAssignNamesToExcludedNames get() = this.joinToString { "${it.name} = EXCLUDED.${it.name}" }
