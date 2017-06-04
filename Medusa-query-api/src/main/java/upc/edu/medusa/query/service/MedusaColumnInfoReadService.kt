package upc.edu.medusa.query.service

import upc.edu.medusa.query.model.MedusaColumnInfo

/**
 * Created by msi on 2017/4/19.
 */
interface MedusaColumnInfoReadService {
    /**
     * 查询表中列全部字段信息,前台使用
     */
    fun queryColumnInfo(tableName: String, databaseName: String): List<MedusaColumnInfo>

    /**
     * 查询单个列字段详细信息
     */
    fun queryColumnInfoDetail(columnName: String, tableName: String, databaseName: String): MedusaColumnInfo

    /**
     * 查询列字段详细信息,通过Tid查询
     */
    fun queryColumnInfoDetailByTid(tid: Long, columnName: String): MedusaColumnInfo
}