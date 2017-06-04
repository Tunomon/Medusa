package upc.edu.medusa.datasource.model

import java.io.File

/**
 * Created by msi on 2017/5/14.
 */
class SqoopArgs {
    companion object {
        val SQOOP = "sqoop "                                            //指定sqoop命令
        val IMPORT = "import"                                           //导入模式
        val EXPORT = "export"                                           //导出模式
        val IMPORT_ALL_TABLES = "import-all-tables"                     //整库导入模式
        val CONNECT = "--connect"                                       //链接url
        val USERNAME = "--username"                                     //数据库用户名
        val PASSWORD = "--password"                                     //数据库密码
        val TABLE = "--table"                                           //指定表名
        val HIVE_TABLE = "--hive-table"                                 //hive中的表名
        val HIVE_DATABASE = "--hive-database"                           //hive中的数据库名
        val EXCLUDE_TABLES = "--exclude-tables"                         //整库模式中指定不需要导入的表
        val OUTDIR = "--outdir"                                         //Java文件导出目录, 默认${HOME}
        val DELETE_TARGET_DIR = "--delete-target-dir"                   //是否删除HDFS目录, 不指定: 当hdfs里存在此表临时目录时会报错
        val HIVE_IMPORT = "--hive-import"                               //指定导入到Hive
        val HIVE_OVERWRITE = "--hive-overwrite"                         //是否覆盖Hive原有数据, 不指定: 数据以拼接形式插入到Hive
        val NULL_STRING = """--null-string"""                           //string类型转换
        val DEFAULT_NULL_STRING = """\\N"""                             //默认string类型为NULL
        val NULL_NON_STRING = """--null-non-string"""                   //非string类型转换
        val DEFAULT_NULL_NON_STRING = """\\N"""                         //默认非string类型为NULL
        val MAPPERS = "-m"                                              //指定MapReduce的Map并发个数, 默认 4
        val DIRECT = "--direct"                                         //指定直连模式, 提高dump效率, 但不是对所有数据源都支持, 支持MySQL, 其他慎用
        val UTF8 = "-- --default-character-set=utf-8"                   //指定UTF-8编码, 不可与 --direct 同时使用
        val FIELDS_TERMINATED_BY = """--fields-terminated-by"""         //字段间的分隔符
        val DEFAULT_FIELDS_TERMINATED_BY = """"\0001""""                //默认字段间的分隔符为"\0001"
        val LINES_TERMINATED_BY = """--lines-terminated-by"""           //行间的分隔符
        val DEFAULT_LINES_TERMINATED_BY = """'\n'"""                    //默认行间的分隔符为'\n'
        val WHERE = "--where"                                           //查询条件
        val DROP_IMPORT_DELIMS = "--hive-drop-import-delims"            //删除表中的/n,/r,/01字段
        val HIVE_PARTITION_KEY = "--hive-partition-key"                 //分区键,默认为pt
        val HIVE_PARTITION_VALUES = "--hive-partition-value"            //分区键的值
        val COMPRESS = "--compress"                                     //启动压缩,默认为gzip压缩算法,导入后的文件为.gz后缀
        val COMPRESSION_CODEC = "--compression-codec"                   //指定压缩格式
        val SNAPPY = "snappy"                                           //此项目使用的压缩格式
        val QUERY = "--query"                                           //使用sql语句查询
        val TARGET_DIR = "--target-dir"                                 //指定目标目录
        val ENCODING = "--encoding"                                     //指定己方数据库编码格式Encoding，修改源码所得
        val PARQUETFILE = "--as-parquetfile"                            //存储为parquetfile格式的文件

        val JAVA_OUTDIR = System.getenv("\${HOME}") + File.separator + ".sqoop" + File.separator + "java"   //Java文件导出目录

        //以下为EXPORT

        val EXPORT_DIR = "--export-dir"                    //指定存放数据的HDFS的源目录
        val INPUT_FIELDS_TERMINATED_BY = """--input-fields-terminated-by"""      //以字段间的分隔符来解析得到各字段值
        val DEFAULT_INPUT_FIELDS_TERMINATED_BY = """"\0001""""                   //默认字段间分隔符的值为\0001
        val INPUT_LINES_TERMINATED_BY = """--input-lines-terminated-by"""        //以每条记录行之间的分隔符来解析得到字段值
        val DEFAULT_INPUT_LINES_TERMINATED_BY = """'\n'"""                       //默认每条记录行之间的分隔符为'\n'
        val INPUT_NULL_STRING = """--input-null-string"""                        //默认string类型转换为NULL的字段
        val DEFAULT_INPUT_NULL_STRING = """\\N"""                                //默认string类型由'\\N'转换为NULL
        val INPUT_NULL_NON_STRING = """--input-null-non-string"""                //默认非string类型转换为NULL的字段
        val DEFAULT_INPUT_NULL_NON_STRING = """\\N"""                            //默认非string类型由'\\N'转换为NULL
        val INPUT_ESCAPED_BY = """--input-escaped-by"""                          //对含有转义的字段值作转义处理,慎用,了解后再用。
        val DEFAULT_INPUT_ESCAPED_BY = """\\"""                                  //默认转义字符为\
        val UPDATE_MODE = "--update-mode"                                        //更新模式
        val UPDATE_MODE_INSERT = "allowinsert"                                   //设置默认更新模式为allowinsert
        val UPDATE_KEY = "--update-key"                                          //用户设置唯一键、更新列
        val COLUMNS = "--columns"                                                //设置导入到关系型数据库的列的顺序
        val RECURSIVE_EXPORT = "--recursive-export"                              //支持子目录递归导出，全量导出
    }
}