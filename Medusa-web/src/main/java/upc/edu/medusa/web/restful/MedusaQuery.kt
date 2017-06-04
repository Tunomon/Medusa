package upc.edu.medusa.web.restful

import com.google.common.base.Throwables
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import upc.edu.medusa.datasource.model.MedusaDatasource
import upc.edu.medusa.datasource.service.MedusaDatasourceReadService
import upc.edu.medusa.query.hive.HiveSourceSqlQuery
import upc.edu.medusa.query.model.MedusaColumnInfo
import upc.edu.medusa.query.model.MedusaTableInfo
import upc.edu.medusa.query.service.MedusaColumnInfoReadService
import upc.edu.medusa.query.service.MedusaColumnInfoWriteService
import upc.edu.medusa.query.service.MedusaTableInfoReadService
import upc.edu.medusa.query.service.MedusaTableInfoWriteService
import upc.edu.medusa.query.utils.SqlQueryConvertUtils

/**
 * Created by msi on 2017/4/19.
 */
@RequestMapping("/api/query")
@RestController
class MedusaQuery @Autowired constructor(
        private val medusaTableInfoReadService: MedusaTableInfoReadService,
        private val medusaTableInfoWriteService: MedusaTableInfoWriteService,
        private val medusaColumnInfoReadService: MedusaColumnInfoReadService,
        private val medusaColumnInfoWriteService: MedusaColumnInfoWriteService,
        val userSqlQuery: HiveSourceSqlQuery
) {

    /**
     * 更新表信息,所属部门、所属业务、数据描述
     */
    @RequestMapping("/column/info/update", method = arrayOf(RequestMethod.PUT))
    fun updateTableInfo(@RequestBody medusaColumn: MedusaColumnInfo): MedusaColumnInfo {
        val meduasaColumnInfo: MedusaColumnInfo = MedusaColumnInfo()
        return meduasaColumnInfo
    }

    /**
     * 查询单个表信息
     */
    @RequestMapping("/table/info", method = arrayOf(RequestMethod.GET))
    fun queryTableInfo(@RequestParam("tableName") tableName: String,
                       @RequestParam("databaseBelongs") databaseBelongs: String): MedusaTableInfo {
        val medusaTableInfo = medusaTableInfoReadService.queryTableInfoByName(tableName, databaseBelongs)
        if (medusaTableInfo == null) {
            if (medusaTableInfoWriteService.saveTableInfo(tableName, databaseBelongs))
                return medusaTableInfoReadService.queryTableInfoByName(tableName, databaseBelongs)!!
            throw Exception("save.table.info.failed.when.it.does.not.exist")
        }
        return medusaTableInfo
    }

    /**
     * 查询单个字段详细信息
     */
    @RequestMapping("/column/info/detail", method = arrayOf(RequestMethod.GET))
    fun queryColumnInfoDetail(@RequestParam("tableName") tableName: String,
                              @RequestParam("columnName") columnName: String,
                              @RequestParam("databaseBelongs") databaseBelongs: String): MedusaColumnInfo {
        val meduasaColumnInfo: MedusaColumnInfo = MedusaColumnInfo()
        return meduasaColumnInfo
    }

    /**
     * 查询全部表信息
     */
    @RequestMapping("/table/infos", method = arrayOf(RequestMethod.GET))
    fun queryTableInfos(): List<MedusaTableInfo> {
        val medusaTableInfos = medusaTableInfoReadService.queryTableInfos()
        return medusaTableInfos
    }

    /**
     * 查询一个表的全部字段信息
     */
    @RequestMapping("/column/infos", method = arrayOf(RequestMethod.GET))
    fun queryColumnInfos(@RequestParam("tableName") tableName: String,
                         @RequestParam("databaseBelongs") databaseBelongs: String): List<MedusaColumnInfo> {
        val medusaColumnInfos = medusaColumnInfoReadService.queryColumnInfo(tableName, databaseBelongs)
        if (medusaColumnInfos.isEmpty()) {
            return medusaColumnInfoWriteService.saveColumnInfo(databaseBelongs, tableName)
        }
        return medusaColumnInfos
    }

    /**
     * 实时查询全部表信息
     */
    @RequestMapping("/hive/table/infos", method = arrayOf(RequestMethod.GET))
    fun queryFromHiveTableInfos(): List<MedusaTableInfo> {
        val medusaTableInfos: MutableList<MedusaTableInfo> = mutableListOf()
//        val medusaTableInfos = mutableListOf<MedusaTableInfo>()
        val dbQueryResult = userSqlQuery.executeQuerySql(0L, "", "", SqlQueryConvertUtils.showDatabases())
        //将所有的数据库存储起来
        val allDatabases = dbQueryResult.data.map { p -> p["database_name"] }
        for (database in allDatabases) {
            //若此库没有表则跳过此库
            val queryResult = userSqlQuery.executeQuerySql(0L, "", database.toString(), SqlQueryConvertUtils.showTables())
            if (queryResult.data.isEmpty()) continue
            val allTables = queryResult.data.map { p -> p["tab_name"] }
            for (table in allTables) {
                val medusaTable: MedusaTableInfo = MedusaTableInfo()
                medusaTable.databaseBelongs = database.toString()
                medusaTable.name = table.toString()
                medusaTableInfos.add(medusaTable)
            }
        }
        return medusaTableInfos
    }


}