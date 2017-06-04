package upc.edu.medusa.datasource.service

import upc.edu.medusa.datasource.model.MedusaDatasource

/**
 * Created by msi on 2017/4/19.
 */
interface MedusaDatasourceReadService {

    /**
     * 查询数据源列表
     */
    fun findDatasources(): List<MedusaDatasource>

    /**
     * 查询数据源详细信息
     */
    fun findDatasourceByname(name: String?): MedusaDatasource

    /**
     * 根据id查询数据源
     * @param datasourceId 数据源id
     * @return 数据源
     */
    fun findDatasourceById(datasourceId: Long): MedusaDatasource

    /**
     * 数据源测试连通
     */
    fun testConnection(dataSource: MedusaDatasource): Boolean
}