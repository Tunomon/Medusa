package upc.edu.medusa.query

import upc.edu.medusa.query.dto.MedusaUserSqlQueryResultDto

/**
 * Created by msi on 2017/5/16.
 */
public interface UserSqlQuery {

    /**
     * 执行对应的数据库接口数据信息内容
     */
    fun executeQuerySql(userId: Long, uuid: String?, database: String, sql: String, needResultSet: Boolean = true): MedusaUserSqlQueryResultDto

    /**
     * 执行对应的SQl 语句内容
     */
    fun executeSql(userId: Long, uuid: String?, database: String, sql: String): Boolean

    /**
     * 执行多条sql,返回最后一条执行语句的执行结果
     */
    fun executeSqls(userId: Long, uuid: String?, database: String, sqls: List<String>, needResultSet: Boolean = true): MedusaUserSqlQueryResultDto
}
