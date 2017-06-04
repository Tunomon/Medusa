package upc.edu.medusa.query.service

import com.avaje.ebean.Ebean
import com.google.common.base.Throwables
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import upc.edu.medusa.query.hive.HiveSourceSqlQuery
import upc.edu.medusa.query.model.MedusaTableInfo
import upc.edu.medusa.query.utils.SqlQueryConvertUtils

/**
 * Created by msi on 2017/5/5.
 */
@Service
class MedusaTableInfoWriteServiceImpl @Autowired constructor(
        val userSqlQuery: HiveSourceSqlQuery,
        val tableInfoReadService: MedusaTableInfoReadService
) : MedusaTableInfoWriteService {

    override fun updateTableInfo(tableName: String, databaseName: String): Boolean {
        val tableInfo = tableInfoReadService.queryTableInfoByName(tableName, databaseName)!!
        val newTableInfo = queryFromHive(0L, databaseName, tableName)
        tableInfo.storageSpace = newTableInfo.storageSpace
        tableInfo.isPartition = newTableInfo.isPartition
        tableInfo.isExternal = newTableInfo.isExternal
        tableInfo.location = newTableInfo.location
        Ebean.update(tableInfo)
        return true
    }

    override fun saveTableInfo(tableName: String, databaseName: String): Boolean {
        val tableInfo = queryFromHive(0L, databaseName, tableName)
        Ebean.save(tableInfo)
        return true
    }


    /**
     * 从Hive中查询信息
     */
    private fun queryFromHive(userId: Long, databaseName: String, tableName: String): MedusaTableInfo {

        val tableInfo = MedusaTableInfo()
        //查表的基本信息
        getBasicInfo(tableInfo, userId, databaseName, tableName)
        getLocation(tableInfo, userId, databaseName, tableName)
        getExternal(tableInfo, userId, databaseName, tableName)
        getPartitionInfo(tableInfo, userId, databaseName, tableName)
        return tableInfo
    }


    /**
     * 表得基本信息
     */
    private fun getBasicInfo(tableInfo: MedusaTableInfo, userId: Long, databaseName: String, tableName: String): MedusaTableInfo {
        val dataes = userSqlQuery.executeQuerySql(userId, "", databaseName, SqlQueryConvertUtils.showTblProperties(tableName), true)!!.data
        val columns = userSqlQuery.executeQuerySql(userId, "", databaseName, SqlQueryConvertUtils.showTblProperties(tableName), true)!!.columns
        var map: Map<String, String> = mutableMapOf()
        for (data in dataes) {
            var list: List<String> = mutableListOf()
            for (column in columns) {
                list = list.plus(data[column].toString())
            }
            map = map.plus(Pair(list.first(), list.last()))
        }
        tableInfo.name = tableName
        tableInfo.databaseBelongs = databaseName
        if (tableInfo.tableDesc.isNullOrEmpty()) {
            tableInfo.tableDesc = map["comment"] ?: ""
        }
        val storageSpace = map["totalSize"]?.toLong() ?: 0
        tableInfo.storageSpace = storageSpace
        return tableInfo
    }


    /**
     * 表的分区信息
     */
    private fun getPartitionInfo(tableInfo: MedusaTableInfo, userId: Long, databaseName: String, tableName: String): MedusaTableInfo {
        //查表的分区信息
        try {
            val dataes = userSqlQuery.executeQuerySql(userId, "", databaseName, SqlQueryConvertUtils.descFormattedTable(tableName), true).data
            val columns = userSqlQuery.executeQuerySql(userId, "", databaseName, SqlQueryConvertUtils.descFormattedTable(tableName), true).columns
            var map: Map<String, String> = mutableMapOf()
            for (data in dataes) {
                var list: List<String> = mutableListOf()
                for (column in columns) {
                    list = list.plus(data[column].toString())
                }
                map = map.plus(Pair(list.first().trim(), list[1].trim()))
            }
            tableInfo.isPartition = map["# Partition Information"] != null
            return tableInfo
        } catch (e: Exception) {
            System.out.println("failed to update columnInfo , cause by : {}")
            tableInfo.isPartition = null
            return tableInfo
        }
    }

    /**
     * 获得表的Location信息
     */
    private fun getLocation(tableInfo: MedusaTableInfo, userId: Long, databaseName: String, tableName: String): MedusaTableInfo {
        val dataes = userSqlQuery.executeQuerySql(userId, "", databaseName, SqlQueryConvertUtils.descFormattedTable(tableName), true).data
        val columns = userSqlQuery.executeQuerySql(userId, "", databaseName, SqlQueryConvertUtils.descFormattedTable(tableName), true).columns
        var map: Map<String, String> = mutableMapOf()
        for (data in dataes) {
            var list: List<String> = mutableListOf()
            for (column in columns) {
                list = list.plus(data[column].toString())
            }
            map = map.plus(Pair(list.first().trim(), list[1].trim()))
        }
        tableInfo.location = map["Location:"]?.toString()
        return tableInfo
    }

    /**
     * 获得表是否是扩展表
     */
    private fun getExternal(tableInfo: MedusaTableInfo, userId: Long, databaseName: String, tableName: String): MedusaTableInfo {
        val dataes = userSqlQuery.executeQuerySql(userId, "", databaseName, SqlQueryConvertUtils.descFormattedTable(tableName), true).data
        val columns = userSqlQuery.executeQuerySql(userId, "", databaseName, SqlQueryConvertUtils.descFormattedTable(tableName), true).columns
        var map: Map<String, String> = mutableMapOf()
        for (data in dataes) {
            var list: List<String> = mutableListOf()
            for (column in columns) {
                list = list.plus(data[column].toString())
            }
            map = map.plus(Pair(list.first().trim(), list[1].trim()))
        }
        val tableType = map["Table Type:"]?.toString()
        if (tableType?.indexOf("MANAGED")!! >= 0) {
            tableInfo.isExternal = false
        } else if (tableType?.indexOf("EXTERNAL")!! >= 0) {
            tableInfo.isExternal = true
        }
        return tableInfo
    }


}