package jdbcat.core

import java.sql.PreparedStatement

class TemplatizeStatement internal constructor(
    private val preparedStatement: PreparedStatement,
    private val sortedColumns: List<Pair<Column<*>, String>>
) : PreparedStatement by preparedStatement {

    fun setColumns(
        clearParameters: Boolean = true,
        block: (ColumnValueBuilder) -> Unit
    ): TemplatizeStatement {
        val columnValueBuilder = ColumnValueBuilder()
        block(columnValueBuilder)
        if (clearParameters) {
            clearParameters()
        }
        sortedColumns.forEachIndexed { index, columnWithParam ->
            val value = columnValueBuilder.columnToValueMap[columnWithParam]
            columnWithParam.first.setData(statement = this, paramIndex = index + 1, value = value)
        }
        return this
    }

    override fun toString() = "TemplatizeStatement: $preparedStatement"
}
