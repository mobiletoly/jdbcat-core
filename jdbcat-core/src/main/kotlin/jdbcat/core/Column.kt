package jdbcat.core

import java.lang.NullPointerException
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.UUID

abstract class Column<T : Any?> constructor(
    val name: String,
    val type: String,
    val specifier: String? = null,
    val table: Table
) {
    internal val params = linkedMapOf<String,String>()
    operator fun get(paramName: String) = params.getOrPut(paramName) { UUID.randomUUID().toString() }
    val v = get("")

    val sqlIndexName = table.sqlBaseIndexName + name
    val createIndexStatement = "CREATE INDEX $sqlIndexName ON ${table.tableName} ($name)"

    override fun toString() = name
    abstract fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?)
    abstract fun getData(rs: ResultSet, paramIndex: Int): T

    fun def() = "$name $type ${specifier ?: ""}".trimEnd()

    protected fun validateNotNull(rs: ResultSet) {
        if (rs.wasNull()) {
            throw NullPointerException("Column [$name] must not contain NULL values")
        }
    }
}

class ColumnValueBuilder {
    internal val columnToValueMap = mutableMapOf<Pair<Column<*>, String>, Any?>()

    operator fun <T> set(column: Column<T>, paramName: String, value: T) {
        columnToValueMap[column to paramName] = value
    }
    operator fun <T> set(column: Column<T>, value: T) = set(column = column, paramName = "", value = value)
}

class ColumnValueExtractor(
    val resultSet: ResultSet,
    private val columnNameToIndex: Map<String, Int>
) {
    operator fun <V> get(column: Column<V>): V {
        val ind = columnNameToIndex.getValue("${column.table.tableName.toLowerCase()}.${column.name.toLowerCase()}")
        return column.getData(rs = resultSet, paramIndex = ind)
    }
}
