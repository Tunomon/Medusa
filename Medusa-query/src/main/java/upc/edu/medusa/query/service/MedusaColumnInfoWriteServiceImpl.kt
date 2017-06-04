package upc.edu.medusa.query.service

import com.avaje.ebean.Ebean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import upc.edu.medusa.query.dto.MedusaUserTableDescDto
import upc.edu.medusa.query.dto.MedusaUserTableFieldDto
import upc.edu.medusa.query.hive.HiveSourceSqlQuery
import upc.edu.medusa.query.model.MedusaColumnInfo
import upc.edu.medusa.query.model.MedusaTableInfo
import upc.edu.medusa.query.utils.SqlQueryConvertUtils

/**
 * Created by msi on 2017/5/5.
 */
@Service
class MedusaColumnInfoWriteServiceImpl @Autowired constructor(
        val medusaTableInfoReadService: MedusaTableInfoReadService,
        val medusaColumnInfoReadService: MedusaColumnInfoReadService,
        val userSqlQuery: HiveSourceSqlQuery
) : MedusaColumnInfoWriteService {


    override fun saveColumnInfo(horusColumnInfo: MedusaColumnInfo): Boolean {
        Ebean.save(horusColumnInfo)
        return true
    }

    /**
     * 更新列字段信息,通用方法,内只有Ebean.update
     */
    override fun updateColumnInfo(medusaColumnInfo: MedusaColumnInfo): Boolean {
        Ebean.update(medusaColumnInfo)
        return true
    }

    override fun saveColumnInfo(databaseName: String, tableName: String): List<MedusaColumnInfo> {
        val tableInfo = medusaTableInfoReadService.queryTableInfoByName(tableName, databaseName) ?: throw Exception("table.info.is.null")
        val columnInfos = medusaColumnInfoReadService.queryColumnInfo(tableName, databaseName)
        //为空则保存
        if (columnInfos.isEmpty()) {
            return saveAllColumns(databaseName, tableName, tableInfo)
        }
        return updateAllColumns(databaseName, tableName, columnInfos, tableInfo)
    }

    /**
     * 存储全部字段信息
     */
    private fun saveAllColumns(databaseName: String, tableName: String, tableInfo: MedusaTableInfo): List<MedusaColumnInfo> {
        //过滤一下,分区表会多出来几个字段
        val fields = MedusaUserTableDescDto.buildFromSqlQueryResult(userSqlQuery.executeQuerySql(0L, "", databaseName, SqlQueryConvertUtils.descTable(tableName)), tableName).fields

        val filterFields = fields.filter {
            !(it.columnName.startsWith("#") || it.columnName.isNullOrBlank() || it.dataType == "null")
        }.distinctBy(MedusaUserTableFieldDto::columnName)

        var list: List<MedusaColumnInfo> = mutableListOf()
        for ((index, field) in filterFields.withIndex()) {
            val columnInfo = MedusaColumnInfo()
            columnInfo.name = field.columnName
            columnInfo.tid = tableInfo.id
            columnInfo.type = field.dataType
            columnInfo.desc = field.comment
            saveColumnInfo(columnInfo)
            list = list.plus(columnInfo)
        }
        return list
    }


    /**
     * 取Hive中表字段与MySql存储的表字段的交集,如果Hive中字段包括MySql中存的字段,则更新,否则删除MySql中存储的字段;
     * 如果MySql中字段包括Hive中存的字段,则不做任何操作(因为已更新过),否则存储Hive中存在而MySql中不存在的字段
     */
    private fun updateAllColumns(databaseName: String, tableName: String, columnInfo: List<MedusaColumnInfo>, tableInfo: MedusaTableInfo): List<MedusaColumnInfo> {
        val hiveFields = MedusaUserTableDescDto.buildFromSqlQueryResult(userSqlQuery.executeQuerySql(0L, "", databaseName, SqlQueryConvertUtils.descTable(tableName)), tableName).fields
        //过滤一下,分区表会多出来几个字段
        val filterFields = hiveFields.filter {
            !(it.columnName.startsWith("#") || it.columnName.isNullOrBlank() || it.dataType == "null")
        }.distinctBy(MedusaUserTableFieldDto::columnName)

        //进行更新操作
        val mySqlFields = columnInfo
        var hiveLists: List<MedusaColumnInfo> = mutableListOf()
        //将从Hive中取出的数据先转换成List封装的HorusColumnInfo对象
        filterFields.forEachIndexed { i, it ->
            val column: MedusaColumnInfo = MedusaColumnInfo()
            column.name = it.columnName
            if (column.desc.isNullOrEmpty()) {
                column.desc = it.comment
            }
            column.type = it.dataType
            column.tid = tableInfo.id
            hiveLists = hiveLists.plus(column)
        }
        //三个List用来存放结果
        var differentSave: List<MedusaColumnInfo> = mutableListOf()
        var differentDel: List<MedusaColumnInfo> = mutableListOf()
        var intersection: List<MedusaColumnInfo> = mutableListOf()

        //mysql中有的,hive中没得,放入删除列表中,都有的放在交集中更新
        for (my in mySqlFields) {
            var continueLoop = false
            for (hive in hiveLists) {
                if (my.name == hive.name) {
                    intersection = intersection.plus(hive)
                    continueLoop = true
                    break
                }
            }
            if (continueLoop) continue
            else differentDel = differentDel.plus(my)
        }
        //hive中有的,mysql中没得,放入存储列表中,都有的什么都不做
        for (hive in hiveLists) {
            var continueLoop = false
            for (my in mySqlFields) {
                if (my.name == hive.name) {
                    continueLoop = true
                    break
                }
            }
            if (continueLoop) continue
            else differentSave = differentSave.plus(hive)
        }
        //进行删除、存储、更新操作
        dropFromMysql(differentDel)
        saveFromHive(differentSave)
        updateFromHive(intersection)


        val columnInfos = medusaColumnInfoReadService.queryColumnInfo(tableName, databaseName)
        return columnInfos
    }

    /**
     * 若Mysql中的字段Hive中没有,则删除该条MySql中的字段
     */
    private fun dropFromMysql(differentDel: List<MedusaColumnInfo>) {
        differentDel.forEach {
            val tid = it.tid!!
            val columnName = it.name

            val medusaColumn = medusaColumnInfoReadService.queryColumnInfoDetailByTid(tid, columnName)
            Ebean.delete(medusaColumn)
        }
    }


    /**
     * 若Hive中的字段Mysql中没有,则保存该条字段
     */
    private fun saveFromHive(differentSave: List<MedusaColumnInfo>) {
        differentSave.forEach {
            val horusColumnInfo = MedusaColumnInfo()
            horusColumnInfo.name = it.name
            horusColumnInfo.type = it.type
            horusColumnInfo.desc = it.desc
            horusColumnInfo.tid = it.tid
            saveColumnInfo(horusColumnInfo)
        }

    }

    /**
     * 若Hive中的字段Mysql中有,则更新该条字段
     */
    private fun updateFromHive(intersection: List<MedusaColumnInfo>) {
        intersection.forEach {
            val horusColumnInfo = medusaColumnInfoReadService.queryColumnInfoDetailByTid(it.tid!!, it.name)
            horusColumnInfo.name = it.name
            horusColumnInfo.type = it.type
            horusColumnInfo.tid = it.tid
            updateColumnInfo(horusColumnInfo)
        }

    }
}