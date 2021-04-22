package jdbcat.core

import java.sql.ResultSet
import java.sql.ResultSetMetaData

open class Table constructor(val tableName: String) {
    // TODO deal with table names with spaces

    internal val sqlBaseIndexName = "idx_" + tableName + "_"

    private val _columns = mutableListOf<Column<*>>()
    val columns: Collection<Column<*>> get() = _columns

    fun <T : Column<*>> registerColumn(column: T): T {
        _columns.add(column)
        return column
    }

    fun <T : Column<*>> unregisterColumn(column: T): Table {
        _columns.remove(column)
        return this
    }
}

open class EphemeralTable : Table("")

val Collection<Column<*>>.sqlNames get() = this.joinToString(", ") { it.name }
val Collection<Column<*>>.sqlValues get() = this.joinToString(", ") { it.v }
val Collection<Column<*>>.sqlAssignNamesToValues get() = this.joinToString(", ") { "${it.name} = ${it.v}" }
val Collection<Column<*>>.sqlDefinitions get() = this.joinToString(", ") { it.def() }

/**
 * Convert ResultSet to Kotlin's sequence. Be aware that this is asynchronous function,
 * so you always want to handle it results inside .tx/.txAsync() {}
 * Continue processing results of .asSequence() outside of any transaction context (e.g. when top-most
 * context is finished) - will result in "This ResultSet is closed" exception (or something similar).
 * If you need to pass results back from function and you are not sure that caller is running inside
 * a transaction context - use ResultSet.asList() call instead.
 */
fun ResultSet.asSequence() = sequence {
    val columnNameToIndex = resultSetMetadata(this@asSequence.metaData)
    while (this@asSequence.next()) {
        yield(ColumnValueExtractor(resultSet = this@asSequence, columnNameToIndex = columnNameToIndex))
    }
    this@asSequence.close()
}

fun ResultSet.iterator() = this.asSequence().iterator()

fun ResultSet.asList() = this.asSequence().toList()

/** Extract first row and close result set. */
fun <T> ResultSet.singleRowOrNull(block: (ColumnValueExtractor) -> T): T? = use {
    val columnNameToIndex = resultSetMetadata(this.metaData)
    if (this.next()) {
        block.invoke(ColumnValueExtractor(resultSet = it, columnNameToIndex = columnNameToIndex))
    } else {
        null
    }
}

fun <T> ResultSet.singleRow(block: (ColumnValueExtractor) -> T) = this.singleRowOrNull(block)!!

private fun resultSetMetadata(metadata: ResultSetMetaData): MutableMap<String, Int> {
    val columnNameToIndex = mutableMapOf<String,Int>()
    val columnCount = metadata.columnCount
    for (i in 1..columnCount) {
        val tableName = metadata.getTableName(i).toLowerCase()
        val columnName = metadata.getColumnName(i).toLowerCase()
        //val prefix = if (tableName.isEmpty()) "" else "$tableName."
        columnNameToIndex["$tableName.$columnName"] = i
    }
    return columnNameToIndex
}
