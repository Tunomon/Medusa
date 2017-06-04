package upc.edu.medusa.query.hive

import com.google.common.base.Throwables
import upc.edu.medusa.query.UserSqlQuery
import org.apache.commons.dbcp.DelegatingStatement
import org.apache.hive.jdbc.HiveStatement
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import upc.edu.medusa.common.util.Constants
import upc.edu.medusa.query.dto.MedusaUserSqlQueryResultDto
import java.sql.Connection
import java.sql.ResultSet

/**
 * Created by msi on 2017/5/16.
 */
@Component
class HiveSourceSqlQuery @Autowired constructor(
        val hiveDatasourceFactory: HiveDatasourceFactory) : UserSqlQuery {


    /**
     * 执行多条sql,返回最后一条执行语句的执行结果
     */
    override fun executeSqls(userId: Long, uuid: String?, database: String, sqls: List<String>, needResultSet: Boolean): MedusaUserSqlQueryResultDto {
        var connection: Connection? = null

        var hiveStatement: HiveStatement? = null

        var resultSet: ResultSet? = null

        var columns: List<String> = mutableListOf()

        var data: List<Map<String, Any>> = mutableListOf()

        try {
            connection = hiveDatasourceFactory.queryDatasource(database).connection
            var statement = connection.createStatement() as DelegatingStatement
            hiveStatement = statement.innermostDelegate as HiveStatement
            if (needResultSet) {

                for ((index, sql) in sqls.withIndex()) {
                    if ((index == (sqls.size - 1))
                            && !sql.contains(Constants.CREATE_REG)
                            && !sql.contains(Constants.INSERT_INTO_REG)
                            && !sql.contains(Constants.DROP_REG)
                            && !sql.contains(Constants.ALTER_REG)
                            && !sql.contains(Constants.UPDATE_REG)
                            && !sql.contains(Constants.TRUNCATE_REG)
                            && !sql.contains(Constants.LOCK_TABLE_REG)
                            && !sql.contains(Constants.UNLOCK_TABLE_REG)
                            && !sql.contains(Constants.INSERT_OVERWRITE_REG)
                            && !sql.contains(Constants.CREATE_DATABASE_REG)) {
                        resultSet = hiveStatement.executeQuery(sql)
                        //analyse columns

                        val metaData = resultSet.metaData
                        for (index in 1..metaData.columnCount) {
                            columns = columns.plus(metaData.getColumnName(index))
                        }

                        //analyse data
                        while (resultSet.next()) {
                            var rowBean: Map<String, Any> = mutableMapOf()
                            for (s in columns) {
                                var index: Int = findColumn(s, columns)
                                rowBean = rowBean.plus(Pair(s, resultSet.getObject(index)))
                            }
                            data = data.plus(rowBean)
                        }
                    } else {
                        hiveStatement.execute(sql)
                    }
                }

            } else {
                for (sql in sqls) {
                    hiveStatement.execute(sql)
                }
            }

            // fetch log
            return MedusaUserSqlQueryResultDto(columns, data, columns.size, data.size, hiveStatement.queryLog)

        } catch (e: Exception) {
            throw IllegalStateException(e.message)
        } finally {
            try {
                // cals connection
                resultSet?.close()
                hiveStatement?.close()
                connection?.close()
            } catch (e: Exception) {
                System.out.println("close sql connection exception")
            }
        }

    }

    override fun executeQuerySql(userId: Long, uuid: String?, database: String, sql: String, needResultSet: Boolean): MedusaUserSqlQueryResultDto {
        return executeSqls(userId, uuid, database, listOf(sql), needResultSet)
    }

    private fun findColumn(name: String, columns: List<String>): Int {
        return columns.indexOf(name) + 1
    }

    override fun executeSql(userId: Long, uuid: String?, database: String, sql: String): Boolean {
        var connection: Connection? = null

        var hiveStatement: HiveStatement? = null

        try {
            connection = hiveDatasourceFactory.queryDatasource(database).connection
            var statement = connection.createStatement() as DelegatingStatement
            hiveStatement = statement.innermostDelegate as HiveStatement
            hiveStatement.execute(sql)
            return true
        } catch (e: Exception) {
            throw IllegalStateException(e.message)
        } finally {
            try {
                hiveStatement?.close()
                connection?.close()
            } catch (e: Exception) {
                System.out.println("close sql connection exception")
            }
        }
    }
}
