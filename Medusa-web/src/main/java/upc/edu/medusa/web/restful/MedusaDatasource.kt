package upc.edu.medusa.web.restful

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import upc.edu.medusa.datasource.model.MedusaDatasource
import upc.edu.medusa.datasource.service.MedusaDatasourceReadService
import upc.edu.medusa.datasource.service.MedusaDatasourceWriteService
import upc.edu.medusa.hadoop.service.HdfsReadServcie
import upc.edu.medusa.oozie.service.OozieService
import upc.edu.medusa.query.service.MedusaColumnInfoWriteService
import upc.edu.medusa.query.service.MedusaTableInfoWriteService
import upc.edu.medusa.web.restful.service.SqoopArgsService

/**
 * Created by msi on 2017/4/19.
 */
@RestController
@RequestMapping("/api/datasources")
class MedusaDatasource @Autowired constructor(
        private val medusaDatasourceReadService: MedusaDatasourceReadService,
        private val medusaDatasourceWriteService: MedusaDatasourceWriteService
) {

    @RequestMapping("", method = arrayOf(RequestMethod.GET))
    fun queryDatasourcesList(): List<MedusaDatasource> {
        val medusaDatasources = medusaDatasourceReadService.findDatasources()
        for (medusaDatasource in medusaDatasources) {
            medusaDatasource.typeString = MedusaDatasource.Type.from(medusaDatasource.type)!!.desc
        }
        return medusaDatasources
    }

//    @RequestMapping("/find", method = arrayOf(RequestMethod.GET))
//    fun query(@RequestParam(required = false) type: Int?,
//              @RequestParam(required = false) tagIds: String?,
//              @RequestParam(required = false) name: String?): MedusaDatasource? {
//        val medusaDatasource: MedusaDatasource = MedusaDatasource()
//        return medusaDatasource
//    }

    @RequestMapping("/{id}", method = arrayOf(RequestMethod.GET))
    fun queryDetailById(@PathVariable id: Long): MedusaDatasource {
        val medusaDatasource = medusaDatasourceReadService.findDatasourceById(id)
        return medusaDatasource
    }

    @RequestMapping("/{name}", method = arrayOf(RequestMethod.GET))
    fun queryDetailByName(@PathVariable name: String): MedusaDatasource {
        val medusaDatasource = medusaDatasourceReadService.findDatasourceByname(name)
        return medusaDatasource
    }

    /**
     * 创建数据源
     */
    @RequestMapping("/save", method = arrayOf(RequestMethod.POST))
    fun create(@RequestParam("name") name: String,
               @RequestParam("host") host: String,
               @RequestParam("port") port: Int,
               @RequestParam("type") type: Int,
               @RequestParam("username") username: String,
               @RequestParam("password") password: String): Boolean {
        val medusaDatasourcre = MedusaDatasource()
        medusaDatasourcre.name = name
        medusaDatasourcre.host = host
        medusaDatasourcre.port = port
        medusaDatasourcre.type = type
        val typeInfo = MedusaDatasource.Type.from(type)!!
        medusaDatasourcre.driver = typeInfo.driver
        medusaDatasourcre.url = typeInfo.connect + host + ":" + port + "/" + name
        medusaDatasourcre.dbUsername = username
        medusaDatasourcre.dbPassword = password
        return medusaDatasourceWriteService.saveDatasource(medusaDatasourcre)

    }

    /**
     * 更新数据源
     */
    @RequestMapping("/update/{id}", method = arrayOf(RequestMethod.PUT))
    fun update(@PathVariable id: Int): Boolean? {
        return true
    }

    /**
     * 删除数据源
     */
    @RequestMapping("/delete/{id}", method = arrayOf(RequestMethod.POST))
    fun delete(@PathVariable id: Long): Boolean? {
        return medusaDatasourceWriteService.deleteDatasource(id)
    }
}