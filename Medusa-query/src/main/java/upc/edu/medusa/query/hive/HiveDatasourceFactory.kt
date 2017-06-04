package upc.edu.medusa.query.hive

import com.google.common.collect.Maps
import org.apache.commons.dbcp.BasicDataSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.util.concurrent.ConcurrentMap
import javax.sql.DataSource
import upc.edu.medusa.query.HorusQueryHiveProperties


/**
 * Created by msi on 2017/5/16.
 */
@Service
class HiveDatasourceFactory @Autowired constructor(val horusQueryHiveProperties: HorusQueryHiveProperties) {

    private val sourceMap: ConcurrentMap<String, DataSource> = Maps.newConcurrentMap()

    fun queryDatasource(databaseName: String): DataSource {
        // get database
        if (!sourceMap.containsKey(databaseName)) {
            sourceMap[databaseName] = buildDatasource(
                    Database(databaseName, "${horusQueryHiveProperties.url}${databaseName}",
                            horusQueryHiveProperties.username, horusQueryHiveProperties.password))
        }
        return sourceMap[databaseName]!!
    }

    private fun buildDatasource(database: Database): DataSource {
        val datasource: BasicDataSource = BasicDataSource()
        datasource.driverClassName = database.driverName
        datasource.url = database.url
        datasource.username = database.username
        datasource.password = database.password
        datasource.maxActive = database.maxActive
        datasource.maxIdle = database.maxIdle
        datasource.defaultAutoCommit = database.defaultAutoCommit
        datasource.initialSize = database.initialSize
        datasource.timeBetweenEvictionRunsMillis = database.timeBetweenEvictionRunsMillis
        datasource.minEvictableIdleTimeMillis = database.minEvictableIdleTimeMillis
        datasource.testOnBorrow = true
        datasource.testWhileIdle = true
        datasource.removeAbandoned = true
        datasource.logAbandoned = true
        datasource.validationQuery = "SELECT 1"
        return datasource
    }

    /**
     * hive 数据源配置实体类, 对应不同的Database
     * 默认hiveDriver 驱动
     */
    data class Database(val key: String, val url: String,
                        val username: String, val password: String,
                        val driverName: String = "org.apache.hive.jdbc.HiveDriver",
                        val maxActive: Int = 2, val maxIdle: Int = 2,
                        val defaultAutoCommit: Boolean = false, val initialSize: Int = 2,
                        val timeBetweenEvictionRunsMillis: Long = 600000L,
                        val minEvictableIdleTimeMillis: Long = 1800000L)

}