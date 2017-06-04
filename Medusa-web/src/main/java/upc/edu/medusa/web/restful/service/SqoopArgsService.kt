package upc.edu.medusa.web.restful.service

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.RequestParam
import upc.edu.medusa.datasource.model.MedusaDatasource
import upc.edu.medusa.datasource.service.MedusaSqoopArgsService
import upc.edu.medusa.query.service.MedusaTableInfoReadService

/**
 * Created by msi on 2017/5/14.
 */
@Component
class SqoopArgsService {

    @Autowired
    lateinit var medusaSqoopArgService: MedusaSqoopArgsService

    @Autowired
    lateinit var medusaTableInfoReadService: MedusaTableInfoReadService

    /**
     * 根据sqoop对象来判断导入导出
     */
    fun getImportSqoopObject(datasource: MedusaDatasource,
                             importTable: String,
                             importHiveTable: String,
                             importHiveDatabase: String,
                             partitionKey: String,
                             partitionValue: String,
                             overwrite: String): String {
        // 模式分为import和export，分全量full和增量increment
        // 这个map里存各种参数
        val map = mutableMapOf<String, String>()
        var hiveTable = ""
        // 分区键，不填的话默认给pt
        if (!partitionKey.isNullOrBlank()) {
            map.put("partitionKey", partitionKey)
        } else {
            map.put("partitionKey", "pt")
        }
        // 分区值，不填的默认今天
        if (!partitionValue.isNullOrBlank()) {
            map.put("partitionValue", partitionValue)
        } else {
            map.put("partitionValue", DateTimeFormat.forPattern("yyyy-MM-dd").print(DateTime.now()))
        }
        // 判断hiveDatabase和hiveTable是否为空
        if (importHiveTable.isNullOrBlank()) {
            // 导入到hive中的表名不填默认为s_开头
            // 这块拆分处理主要是为了应对Oracle的表，会带上Schema，如果带上Schema的就先拆开然后去后面的，要是mysql的就不带Schema，
            // 前面那个transformTableName也会带上，所以这里也可以拆开，下面再组一次。
            hiveTable = importHiveDatabase + ".s_" + importTable
        } else {
            // 校验是否s_开头
            if (!importHiveTable.startsWith("s_", true))
                throw IllegalArgumentException("table.prefix.is.illegal")
            hiveTable = importHiveDatabase + "." + importHiveTable
        }
        map.put("importHiveTable", hiveTable)
        map.put("overwrite", overwrite)

        return medusaSqoopArgService.getImportAllTablesToHiveSqoopCommandArgs(datasource, importTable, map)

//        else if (sqoop.operMode.equals("export") && sqoop.synchroMode.equals("full")) {
//            // 基本同上，增加了递归分区导出
//            val horusDatasource = HorusDatasource()
//            horusDatasource.dbName = sqoop.dataBase
//            //增加导出编码
//            horusDatasource.url = "${sqoop.url}?useUnicode=true&characterEncoding=utf-8"
//            horusDatasource.dbUserName = sqoop.dbUserName
//            horusDatasource.dbUserPassword = sqoop.dbPassword
//            val map = mutableMapOf<String, String>()
//            if (sqoop.exportKey != null && sqoop.exportKey!!.isNotEmpty()) {
//                val exportKey = StringBuilder()
//                sqoop.exportKey!!.forEach {
//                    exportKey.append(it)
//                    if (it != sqoop.exportKey!!.last()) {
//                        exportKey.append(",")
//                    }
//                }
//                map.put("exportKey", exportKey.toString())
//            } else {
//                map.put("exportKey", "")
//            }
//            if (sqoop.exportColumns != null && sqoop.exportColumns!!.isNotEmpty()) {
//                val exportColumns = StringBuilder()
//                sqoop.exportColumns!!.forEach {
//                    exportColumns.append(it)
//                    if (it != sqoop.exportColumns!!.last()) {
//                        exportColumns.append(",")
//                    }
//                }
//                map.put("exportColumns", exportColumns.toString())
//            } else {
//                map.put("exportColumns", "")
//            }
//            var location = ResponseHelper.getOrThrow(tableInfoReadService.queryTableInfoByName(sqoop.exportHiveTable!!, sqoop.exportHiveDataBase!!))?.location
//            if (location == null) {
//                tableInfoWriteService.saveTableInfo(sqoop.exportHiveTable!!, sqoop.exportHiveDataBase!!)
//                location = ResponseHelper.getOrThrow(tableInfoReadService.queryTableInfoByName(sqoop.exportHiveTable!!, sqoop.exportHiveDataBase!!))?.location
//            }
//            map.put("location", location!!)
//            return ResponseHelper.getOrThrow(horusSqoopService.getExportAllTableSqoopCommandArg(horusDatasource, sqoop.tableName!!, map))
//        }
    }


    fun getExportSqoopObject(datasource: MedusaDatasource,
                             exportHiveDatabase: String,
                             exportHiveTable: String,
                             exportDatabase: String,
                             exportTable: String): String {
        // 基本同上，增加了递归分区导出
        //增加导出编码
        datasource.url = "${datasource.url}?useUnicode=true&amp;characterEncoding=utf-8"
        val map = mutableMapOf<String, String>()
        val location = medusaTableInfoReadService.queryTableInfoByName(exportHiveTable, exportHiveDatabase)!!.location
        map.put("location", location!!)
        return medusaSqoopArgService.getExportAllTableSqoopCommandArg(datasource, exportTable, map)
    }

}
