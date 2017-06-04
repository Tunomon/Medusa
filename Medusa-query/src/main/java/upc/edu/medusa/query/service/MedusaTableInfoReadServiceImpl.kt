package upc.edu.medusa.query.service

import com.google.common.base.Throwables
import org.springframework.stereotype.Service
import upc.edu.medusa.query.model.MedusaTableInfo

/**
 * Created by msi on 2017/5/5.
 */
@Service
class MedusaTableInfoReadServiceImpl : MedusaTableInfoReadService {

    override fun queryTableInfos(): List<MedusaTableInfo> {
        val tableInfos = MedusaTableInfo.find.where()
                .findList()
        return tableInfos
    }

    override fun queryTableInfoByName(tableName: String, databaseName: String): MedusaTableInfo? {
        val tableInfo = MedusaTableInfo.find.where()
                .eq("name", tableName)
                .eq("databaseBelongs", databaseName)
                .findUnique()
        return tableInfo
    }

    override fun queryTableInfoById(id: Long): MedusaTableInfo {
        val tableInfo = MedusaTableInfo.find.where()
                .eq("id", id)
                .findUnique()!!
        return tableInfo
    }


}