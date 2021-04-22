package jdbcat.core

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.withContext
import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource

/**
 * Since there are could be multiple DataSource objects - we need to have a set of thread local connections
 * per each data source.
 */
private val connectionThreadLocals = ConcurrentHashMap<DataSource, ThreadLocal<Connection?>>()

/**
 * Start a transaction or reuse already running transaction if this method is called from another
 * JDBCat's transaction context.
 *
 *  You have to define a thread pool dispatcher with number of thread, e.g.
 *      val dbThreadPoolCtx = newFixedThreadPoolContext(10, "dbThreadPool")
 *      dataSource.tx(dbThreadPoolCtx) { ... }
 *
 *  or you could use  Dispatchers.IO dispatcher that is that is designed for offloading blocking IO
 *  tasks to a shared pool of threads and ensures that additional threads in this pool are created and
 *  are shutdown on demand, e.g.
 *      conn.tx(Dispatchers.IO) { ... }
 *  There is an recommended override tx() available that calls this method with Dispatcher.IO
 *
 *  Inner transactions are supported, but commit will be performed only when the most outer transaction
 *  is complete:
 *  ds.tx {
 *     dbcall_1()
 *     ds.tx {
 *       dbcall_2()
 *       dbcall_3()
 *     }
 *  } // commit of dbcall_1()/dbcall_2()/dbcall_3() operations
 *
 *  That is a great way to have inner calls without worrying that some of the code might have its
 *  own transaction block.
 */
suspend fun <T> DataSource.tx(
    dispatcher: CoroutineDispatcher,
    operation: suspend (Connection) -> T
) = withContext(dispatcher) {
    val threadLocal = connectionThreadLocals.getOrPut(this@tx) { ThreadLocal() }
    val existingConnection = threadLocal.get()
    if (existingConnection == null) {
        // acquire new connection from data source
        this@tx.connection.use { connection ->
            withContext(threadLocal.asContextElement(value = connection)) {
                connection.runTransaction(operation)
            }
        }
    } else {
        operation.invoke(existingConnection)
    }
}

/** Transaction that runs on Dispatchers.IO, which is recommended for JDBC operations */
suspend fun <T> DataSource.tx(operation: suspend (Connection) -> T) =
    this.tx(dispatcher = Dispatchers.IO, operation = operation)

/**
 * Use this function if you need to have access to Connection object, but you don't want to start
 * your own transaction and instead require caller to call you in transaction context.
 * This can be useful if you want to return ResultSet's Sequence object from the method and
 * you want to be absolutely sure that caller started a transaction.
 */
suspend fun <T> DataSource.txRequired(operation: suspend (Connection) -> T): T {
    val existingConnection = connectionThreadLocals[this]?.get()
        ?: throw IllegalStateException("This call must be run inside a JDBCat's transaction")
    return operation.invoke(existingConnection)
}

/** @return true, if transaction is already running; false otherwise. */
fun DataSource.txRunning() = connectionThreadLocals[this]?.get() != null

private suspend fun <T> Connection.runTransaction(
    operation: suspend (Connection) -> T
): T {
    return try {
        operation.invoke(this).also {
            if (! this.autoCommit) {
                this.commit()
            }
        }
    } catch (th: Throwable) {
        if (! this.autoCommit) {
            this.rollback()
        }
        throw th
    }
}
