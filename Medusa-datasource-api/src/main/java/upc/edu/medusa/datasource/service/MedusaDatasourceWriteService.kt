package upc.edu.medusa.datasource.service

import upc.edu.medusa.datasource.model.MedusaDatasource

/**
 * Created by msi on 2017/5/20.
 */
interface MedusaDatasourceWriteService {
    /**
     * 保存数据源
     */
    fun saveDatasource(medusaDatasource: MedusaDatasource): Boolean

    /**
     * 更新数据源
     */
    fun updateDatasource(medusaDatasource: MedusaDatasource): Boolean

    /**
     * 删除数据源
     */
    fun deleteDatasource(id: Long): Boolean
}