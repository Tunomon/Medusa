package upc.edu.medusa.query.service

/**
 * Created by msi on 2017/5/5.
 */
interface MedusaTableInfoWriteService {

    /**
     * 保存表信息
     */
    fun saveTableInfo(tableName: String, databaseName: String): Boolean

    /**
     * 更新表信息
     */
    fun updateTableInfo(tableName: String, databaseName: String): Boolean

}