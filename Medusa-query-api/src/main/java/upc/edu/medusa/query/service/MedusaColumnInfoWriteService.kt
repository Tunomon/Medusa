package upc.edu.medusa.query.service

import upc.edu.medusa.query.model.MedusaColumnInfo

/**
 * Created by msi on 2017/5/5.
 */
interface MedusaColumnInfoWriteService {
    /**
     * 存储列字段信息(若查出来没字段信息则加),hive <-> msql
     */
    fun saveColumnInfo(databaseName: String, tableName: String): List<MedusaColumnInfo>

    /**
     * 保存列字段信息,通用方法,内只有Ebean.save
     */
    fun saveColumnInfo(horusColumnInfo: MedusaColumnInfo): Boolean

    /**
     * 更新列字段信息,通用方法,内只有Ebean.update
     */
    fun updateColumnInfo(medusaColumnInfo: MedusaColumnInfo): Boolean
}