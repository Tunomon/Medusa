package upc.edu.medusa.datasource.service

import com.avaje.ebean.Ebean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import upc.edu.medusa.datasource.model.MedusaDatasource

/**
 * Created by msi on 2017/5/20.
 */
@Service
class MedusaDatasourceWriteServiceImpl @Autowired constructor(
        private val medusaDatasourceReadService: MedusaDatasourceReadService
) : MedusaDatasourceWriteService {

    override fun saveDatasource(medusaDatasource: MedusaDatasource): Boolean {
        Ebean.save(medusaDatasource)
        return true
    }

    override fun updateDatasource(medusaDatasource: MedusaDatasource): Boolean {
        Ebean.update(medusaDatasource)
        return true
    }

    override fun deleteDatasource(id: Long): Boolean {
        Ebean.delete(medusaDatasourceReadService.findDatasourceById(id))
        return true
    }
}