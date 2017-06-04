package upc.edu.medusa.datasource.service

import upc.edu.medusa.datasource.model.MedusaDatasource

/**
 * Created by msi on 2017/5/14.
 */
interface MedusaSqoopArgsService {
    /**
     * 根据数据源生成导出全部表到hive的sqoop命令的arg命令
     * @param datasource 数据源
     * @param excludeTables 排除的表名
     * @return sqoop命令的arg
     */
    fun getImportAllTablesToHiveSqoopCommandArgs(datasource: MedusaDatasource,  table: String, extra: Map<String, String>): String

    /**
     * 根据数据源全量导出表到关系型数据库的sqoop命令的arg模式
     * @param datasource 数据源
     * @param location 需要导出的hive的表在hdfs中的位置
     * @param table 导出到关系型数据库中的表名
     * @return sqoop命令的arg模式
     */
    fun getExportAllTableSqoopCommandArg(datasource: MedusaDatasource, table: String, extra: Map<String, String>): String
}