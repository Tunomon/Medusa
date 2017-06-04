package upc.edu.medusa.query.service

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import upc.edu.medusa.query.model.MedusaColumnInfo

/**
 * Created by msi on 2017/5/5.
 */
@Service
class MedusaColumnInfoReadServiceImpl @Autowired constructor(
        val medusaTableInfoReadService: MedusaTableInfoReadService
) : MedusaColumnInfoReadService {

    override fun queryColumnInfo(tableName: String, databaseName: String): List<MedusaColumnInfo> {
        val tableInfo = medusaTableInfoReadService.queryTableInfoByName(tableName, databaseName) ?: throw Exception("table.info.is.null")
        //表信息不为空时,去查找字段信息
        val columnInfos = MedusaColumnInfo.find.where()
                .eq("tid", tableInfo.id)
                .findList()
        return columnInfos
    }


    override fun queryColumnInfoDetail(columnName: String, tableName: String, databaseName: String): MedusaColumnInfo {
        val tableInfo = medusaTableInfoReadService.queryTableInfoByName(tableName, databaseName) ?: throw Exception("table.info.is.null")
        val columnInfo = queryColumnInfoDetailByTid(tableInfo.id!!, columnName)
        return columnInfo

    }

    override fun queryColumnInfoDetailByTid(tid: Long, columnName: String): MedusaColumnInfo {
        val columnInfo = MedusaColumnInfo.find.where()
                .eq("tid", tid)
                .eq("name", columnName)
                .findUnique()!!
        return columnInfo
    }
}