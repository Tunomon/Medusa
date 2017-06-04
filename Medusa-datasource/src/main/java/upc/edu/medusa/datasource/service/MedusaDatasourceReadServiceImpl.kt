package upc.edu.medusa.datasource.service

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import upc.edu.medusa.datasource.model.MedusaDatasource
import upc.edu.medusa.datasource.util.DBUtil

/**
 * Created by msi on 2017/5/5.
 */
@Service
class MedusaDatasourceReadServiceImpl : MedusaDatasourceReadService {


    /**
     * 查询数据源列表
     */
    override fun findDatasources(): List<MedusaDatasource> {
        val datasource = MedusaDatasource.find.findList()
        return datasource
    }


    override fun findDatasourceByname(name: String?): MedusaDatasource {
        try {
            val datasource = MedusaDatasource.find.where().eq("name", name).findUnique()
            return datasource!!
        } catch (e: Exception) {
            throw Exception("datasource.not.find.by.name")
        }
    }


    override fun findDatasourceById(datasourceId: Long): MedusaDatasource {
        try {
            val datasource = MedusaDatasource.find.where().eq("id", datasourceId).findUnique()
            return datasource!!
        } catch (e: Exception) {
            throw Exception("datasource.not.find.by.id")
        }

    }

    override fun testConnection(dataSource: MedusaDatasource): Boolean {
        try {
            DBUtil.testConnection(dataSource)
            return true
        } catch(e: Exception) {
            throw Exception("test.connertion.dailed")
        }
    }
}