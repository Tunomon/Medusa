package upc.edu.medusa.query.utils

import com.google.common.base.Splitter

/**
 * Created by msi on 2017/5/16.
 */
object SqlQueryConvertUtils {

    private val SPLITTER_SEMICOLON: Splitter = Splitter.on(";").omitEmptyStrings().trimResults();

    private val TMP_TABLE_NAME: String = "tmpTableName"

    fun userDataBase(database: String): String = "use ${database}"

    fun splitSqlBySemicolon(sql: String): Iterable<String> = SPLITTER_SEMICOLON.split(sql)

    fun wrapSqlToLimitResult(sql: String, count: Int): String = "select * from (${sql}) as ${TMP_TABLE_NAME} limit ${count}"

    fun selectSqlLimitResult(table: String, count: Int): String = "select * from ${table} limit ${count}"

    fun showTables(): String = "show tables"

    fun showDatabases(): String = "show databases"

    fun createDatabases(database: String): String = "create database ${database}"

    fun descTable(table: String): String = "desc ${table}"

    fun analyzeTable(table: String): String = "ANALYZE TABLE ${table} COMPUTE STATISTICS NOSCAN"

    fun showPartitions(table: String): String = "show partitions ${table}"

    fun showTableDefaultData(table: String, count: Int) = "select * from ${table} limit ${count}"

    fun showTblProperties(table: String): String = " SHOW TBLPROPERTIES ${table}"

    fun descFormattedTable(table: String): String = "desc formatted ${table}"

    /**
     * @param columnAndTypes 是 新建的表的 column 和 type 组成的字符串
     */
    fun createTable(tableName: String, columnAndTypes: String): String
            = "create table $tableName ($columnAndTypes) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\u0001', 'line.delim'='\n', 'serialization.format'='\u0001' )"

    /**
     * 将表名转换为包含数据库的形式:eg. table1 -> database1.table1
     * 如果表名本身就已经包含数据库名,则不做处理
     */
    fun transformTableName(name: String, database: String): String {
        if (name.contains(".")) return name
        if (!database.isNullOrBlank())
            return "$database.$name"
        else
            return name
    }

    /**
     * @param filePath txt 文件存放的路径
     */
    fun loadTXTToHive(tName: String, filePath: String): String = "load data inpath '$filePath' into table $tName"
}
