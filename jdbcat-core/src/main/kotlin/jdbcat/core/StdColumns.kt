package jdbcat.core

import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

fun Table.varchar(name: String, size: Int, specifier: String? = null) = registerColumn(
    NullableVarCharColumn(name = name, size = size, specifier = specifier, table = this)
)

fun Table.integer(name: String, specifier: String? = null) = registerColumn(
    NullableIntegerColumn(name = name, specifier = specifier, table = this)
)

inline fun <reified T:Enum<T>> Table.enumByName(name: String, size: Int, specifier: String? = null) = registerColumn(
    NullableEnumByNameColumn(name = name, size = size, enumClass = T::class.java, specifier = specifier, table = this)
)

fun Table.boolean(name: String, specifier: String? = null) = registerColumn(
    NullableBooleanColumn(name = name, specifier = specifier, table = this)
)

class NullableVarCharColumn constructor(
    name: String,
    private val size: Int,
    specifier: String? = null,
    table: Table
) : Column<String?>(name = name, type = "varchar($size)", specifier = specifier, table = table) {

    override fun getData(rs: ResultSet, paramIndex: Int): String? = rs.getString(paramIndex)

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        if (value == null) {
            statement.setNull(paramIndex, Types.VARCHAR)
        } else {
            statement.setString(paramIndex, value as String)
        }
    }

    fun nonnull() = table.unregisterColumn(this).registerColumn(
        VarCharColumn(name = name, size = size, specifier = specifier, table = table)
    )
}

class VarCharColumn constructor(
    name: String,
    size: Int,
    specifier: String? = null,
    table: Table
) : Column<String>(name = name, type = "varchar($size) NOT NULL", specifier = specifier, table = table) {

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        statement.setString(paramIndex, value as String)
    }

    override fun getData(rs: ResultSet, paramIndex: Int): String {
        val result = rs.getString(paramIndex)
        validateNotNull(rs)
        return result
    }
}

class NullableIntegerColumn constructor(
    name: String,
    specifier: String? = null,
    table: Table
) : Column<Int?>(name = name, type = "integer", specifier = specifier, table = table) {

    override fun getData(rs: ResultSet, paramIndex: Int): Int? {
        val value = rs.getInt(paramIndex)
        return if (rs.wasNull()) null else value
    }

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        if (value == null) {
            statement.setNull(paramIndex, Types.INTEGER)
        } else {
            statement.setInt(paramIndex, (value as Number).toInt())
        }
    }

    fun nonnull() = table.unregisterColumn(this).registerColumn(
        IntegerColumn(name = name, specifier = specifier, table = table)
    )
}

class IntegerColumn constructor(
    name: String,
    specifier: String? = null,
    table: Table
) : Column<Int>(name = name, type = "integer NOT NULL", specifier = specifier, table = table) {

    override fun getData(rs: ResultSet, paramIndex: Int): Int {
        val result = rs.getInt(paramIndex)
        validateNotNull(rs)
        return result
    }

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        statement.setInt(paramIndex, (value as Number).toInt())
    }
}

class NullableEnumByNameColumn <T:Enum<T>> constructor(
    name: String,
    private val size: Int,
    private val enumClass: Class<T>,
    specifier: String? = null,
    table: Table
) : Column<T?>(name = name, type = "varchar($size)", specifier = specifier, table = table) {

    override fun getData(rs: ResultSet, paramIndex: Int): T? {
        val value = rs.getString(paramIndex) ?: return null
        return enumClass.enumConstants!!.first { it.name == value }!!
    }

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        if (value == null) {
            statement.setNull(paramIndex, Types.VARCHAR)
        } else {
            statement.setString(paramIndex, (value as Enum<*>).name)
        }
    }

    fun nonnull() = table.unregisterColumn(this).registerColumn(
        EnumByNameColumn(name = name, size = size, enumClass = enumClass, specifier = specifier, table = table)
    )
}

class EnumByNameColumn <T:Enum<T>> constructor(
    name: String,
    private val size: Int,
    private val enumClass: Class<T>,
    specifier: String? = null,
    table: Table
) : Column<T>(name = name, type = "varchar($size) NOT NULL", specifier = specifier, table = table) {

    override fun getData(rs: ResultSet, paramIndex: Int): T {
        val value = rs.getString(paramIndex)
        validateNotNull(rs)
        return enumClass.enumConstants!!.first { it.name == value!! }!!
    }

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        statement.setString(paramIndex, (value as Enum<*>).name)
    }
}

class NullableBooleanColumn constructor(
    name: String,
    specifier: String? = null,
    table: Table
) : Column<Boolean?>(name = name, type = "smallint", specifier = specifier, table = table) {

    override fun getData(rs: ResultSet, paramIndex: Int): Boolean? {
        val value = rs.getInt(paramIndex)
        return if (rs.wasNull()) null else value != 0
    }

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        if (value == null) {
            statement.setNull(paramIndex, Types.BOOLEAN)
        } else {
            statement.setShort(paramIndex, if (value as Boolean) 1 else 0)
        }
    }

    fun nonnull() = table.unregisterColumn(this).registerColumn(
        BooleanColumn(name = name, specifier = specifier, table = table)
    )
}

class BooleanColumn constructor(
    name: String,
    specifier: String? = null,
    table: Table
) : Column<Boolean>(name = name, type = "smallint NOT NULL", specifier = specifier, table = table) {

    override fun getData(rs: ResultSet, paramIndex: Int): Boolean {
        val result = rs.getInt(paramIndex)
        validateNotNull(rs)
        return result == 0
    }

    override fun setData(statement: PreparedStatement, paramIndex: Int, value: Any?) {
        statement.setShort(paramIndex, (value as Number).toShort())
    }
}
