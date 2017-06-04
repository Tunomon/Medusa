package upc.edu.medusa.query.service

import upc.edu.medusa.query.model.MedusaTableInfo

/**
 * Created by msi on 2017/5/5.
 */
interface MedusaTableInfoReadService {

    /**
     * 查询全部表信息
     */
    fun queryTableInfos(): List<MedusaTableInfo>

    /**
     * 查询单个表信息
     */
    fun queryTableInfoByName(tableName: String, databaseName: String): MedusaTableInfo?

    /**
     * 根据表ID获取表信息
     */
    fun queryTableInfoById(id: Long): MedusaTableInfo
}