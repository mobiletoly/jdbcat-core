package jdbcat.ext

import jdbcat.core.Column
import jdbcat.core.Table
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.util.Date

/**
 * Create column alias to store time in milliseconds since epoch.
 * As of now works only for database with BIGINT support.
 */

class NullableJavaDateColumn constructor(
    name: String,
    specifier: String? = null,
    table: Table
) : Column<Date?>(name = name, type = "BIGINT", specifier = specifier, table = table) {

    override fun getData(rs: ResultSet, paramIndex: Int): Date {
        return Date(rs.getLong(paramIndex))
    }

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        if (value == null) {
            statement.setNull(paramIndex, Types.BIGINT)
        } else {
            statement.setLong(paramIndex, (value as Date).time)
        }
    }

    fun nonnull() = table.unregisterColumn(this).registerColumn(
        JavaDateColumn(name = name, specifier = specifier, table = table)
    )
}

class JavaDateColumn constructor(
    name: String,
    specifier: String? = null,
    table: Table
) : Column<Date>(name = name, type = "BIGINT NOT NULL", specifier = specifier, table = table) {

    override fun getData(rs: ResultSet, paramIndex: Int): Date {
        return Date(rs.getLong(paramIndex))
    }

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        statement.setLong(paramIndex, (value as Date).time)
    }
}

fun Table.javaDate(name: String, specifier: String? = null) = registerColumn(
    NullableJavaDateColumn(name = name, specifier = specifier, table = this)
)
