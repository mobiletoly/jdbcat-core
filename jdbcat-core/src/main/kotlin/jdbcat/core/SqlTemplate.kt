package jdbcat.core

import java.sql.Connection

/** SQL template for singleRow table. */
fun sqlTemplate(sql: String) = SqlTemplate(template = sql, tables = *arrayOf())

/** SQL template for singleRow table. Table will be exposed via "it" to a caller's lambda. */
fun <T : Table> sqlTemplate(
    table: T,
    block: T.() -> String
): SqlTemplate {
    val sql = block.invoke(table)
    return SqlTemplate(template = sql.trimMargin(), tables = *arrayOf(table as Table))
}

/** SQL template for 2 tables. */
fun <T1 : Table, T2: Table> sqlTemplate(
    table1: T1,
    table2 : T2,
    block: (T1, T2) -> String
): SqlTemplate {
    val sql = block.invoke(table1, table2)
    return SqlTemplate(template = sql.trimMargin(), tables = *arrayOf(table1, table2))
}

/** SQL template for 3 tables. */
fun <T1 : Table, T2: Table, T3: Table> sqlTemplate(
    table1: T1,
    table2 : T2,
    table3 : T3,
    block: (T1, T2, T3) -> String
): SqlTemplate {
    val sql = block.invoke(table1, table2, table3)
    return SqlTemplate(template = sql.trimMargin(), tables = *arrayOf(table1, table2, table3))
}

/** SQL template for 4 tables. */
fun <T1 : Table, T2: Table, T3: Table, T4: Table> sqlTemplate(
    table1: T1,
    table2 : T2,
    table3 : T3,
    table4 : T4,
    block: (T1, T2, T3, T4) -> String
): SqlTemplate {
    val sql = block.invoke(table1, table2, table3, table4)
    return SqlTemplate(template = sql.trimMargin(), tables = *arrayOf(table1, table2, table3, table4))
}

class SqlTemplate(
    private val template: String,
    vararg tables: Table
) {
    internal val sortedColumns: List<Pair<Column<*>, String>>
    val sql: String

    init {
        // Here what we do:
        // Let's assume we have sql = SELECT * WHERE x = ${column2.v} AND y = ${column1.v}
        // As we know ".v" is translated to UUID, so sql looks like (simplified):
        //   SELECT * WHERE x = ABCD-EF-001 AND y = 9867-AB-999
        // We iterate through all tables and columns and record index of each column v's UUID
        // Then we sort columns by index, so in our case we will get a list of [column2, column1]
        // Now we can substitute UUIDs with "?" because that is what JDBC's PreparedStatement understands.
        sortedColumns = tables
            .flatMap { it.columns }
            .map { column ->
                column.params.flatMap { (paramName, uuid) ->
                    val list = mutableListOf<Pair<Int, Pair<Column<*>, String>>>()
                    var startIndex = 0
                    while (true) {
                        val index = template.indexOf(string = uuid, startIndex = startIndex)
                        if (index == -1) {
                            break
                        }
                        list += index to (column to paramName)
                        startIndex = index + uuid.length
                    }
                    list
                }
            }.flatten().asSequence().sortedBy {
                it.first
            }.map {
                it.second
            }.toList()

        var sqlBuilder = template
        sortedColumns.forEach { (column, paramName) ->
            sqlBuilder = sqlBuilder.replace(column[paramName], "?")
        }
        sql = sqlBuilder
    }

    fun prepareStatement(
        connection: Connection,
        returningColumnsOnUpdate: List<Column<*>>? = null
    ): TemplatizeStatement {
        val stmt = if (returningColumnsOnUpdate != null && returningColumnsOnUpdate.isNotEmpty()) {
            connection.prepareStatement(sql, returningColumnsOnUpdate.map { it.name }.toTypedArray())
        } else {
            connection.prepareStatement(sql)
        }
        return TemplatizeStatement(preparedStatement = stmt, sortedColumns = sortedColumns)
    }
}
