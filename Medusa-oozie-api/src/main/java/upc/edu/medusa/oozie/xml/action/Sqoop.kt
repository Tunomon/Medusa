package upc.edu.medusa.oozie.xml.action

import upc.edu.medusa.oozie.utils.escapeXml
import upc.edu.medusa.oozie.xml.node.ActionError
import upc.edu.medusa.oozie.xml.node.ActionOk
import java.io.Serializable

/**
 * Created by msi on 2017/5/9.
 */
class Sqoop() : BaseAction(), Serializable {

    override val xmlLabel: String = "sqoop"
    val xmlns: String = "uri:oozie:sqoop-action:0.2"
    override var name: String? = null
    override var ok: ActionOk? = null
    override var error: ActionError? = null
    var jobTracker: String? = null
    var nameNode: String? = null
    var jobXml: String? = null
    var command: String? = null
    val args: MutableList<String> = mutableListOf() // 与 command 类似, arg 拼接

    //  额外参数
    var dataSource: DataSource? = null
    //    var hiveTable: String? = null
    var ifFast: Boolean = false // 是否是快速的任务节点, 默认false
    var extra: String? = null   // @see HiveSqlParseModel

    companion object {
        val MAPRED_JOB_QUEUE_NAME = "mapred.job.queue.name"

        // share lib path
        val OOZIE_USE_SYSTEM_LIBPATH = "oozie.use.system.libpath"
        val OOZIE_LIBPATH = "oozie.libpath"
    }

    constructor(name: String, ok: String, error: String, jobTracker: String, nameNode: String, jobXml: String, command: String) : this() {
        this.name = name
        this.ok = ActionOk(ok)
        this.error = ActionError(error)
        this.jobTracker = jobTracker
        this.nameNode = nameNode
        this.jobXml = jobXml
        this.command = command
    }

    override fun toXML(): String {
        val xml = StringBuilder("<action name=\"$name\">\n<$xmlLabel xmlns=\"$xmlns\">\n")
        xml.append("<job-tracker>$jobTracker</job-tracker>\n<name-node>$nameNode</name-node>\n")
        xml.append(prepareXML())
        if (!jobXml.isNullOrBlank()) {
            xml.append("<job-xml>$jobXml</job-xml>\n")
        }
        xml.append(configurationXML())
        if (!command.isNullOrBlank()) {
            xml.append("<command>${command.escapeXml()}</command>\n")
        }
        // 参数
        if (args.isNotEmpty()) {
            for (arg in args) {
                xml.append("<arg>${arg.escapeXml()}</arg>\n")
            }
        }
        xml.append("</$xmlLabel>\n")
        xml.append(okAndErrorXML())
        xml.append("</action>\n")
        return xml.toString()
    }

    // 数据源信息 @see HorusDatasource (类似)
    class DataSource {
        var dataSourceId: Long? = null              // 数据源id
        var sourceName: String? = null              // 数据源名称,便于回显
        var type: Int? = null                       // 数据源类型
        var host: String? = null                    // 主机
        var port: Int? = null                       // 端口
        var url: String? = null                     // jdbc url
        var dataBase: String? = null                // 数据库
        var tableName: String? = null               // 表名
        var dbUserName: String? = null              // 用户名
        var dbPassword: String? = null              // 密码

        // 关系型数据源同步配置
        var operMode: String? = null                // 操作方式。 import: 导入, export: 导出
        var synchroMode: String? = null             // 同步模式。 full: 全量, increment: 增量

        // 1. > 导入 import
        var overwrite: Boolean? = null              // 是否覆盖, true or false
        var importHiveTable: String? = null         // 导入的表名称, 前台默认为 's_' + 源表名, 需校验
        var importHiveDatabase: String? = null      // 导入的数据库名称
        var importMore: Boolean? = null             // 导入时高级功能是否开启
        var importQuery: String? = null             // 导入时指定--query的参数
        var importTargetDir: String? = null         // 导入时指定的target-dir,若--query指定,则此项必须指定
        var importEncoding:String? = null           // 导入时指定己方数据库编码，暂时只支持Ehub的ISO8859-1
        // 1.1 >> 全量 full

        // 1.2 >> 增量 increment
        var field: String? = null                       // 增量时间字段名称
        // var fieldType: String? = null                // 字段类型
        var timeOffset: Int? = null                     // 时间偏移量
        //var timeUnit: String? = null                  // 偏移量单元, 'MIN' 分钟 'H' 时 'D' 天 'MON' 月 'Y'年
        //var timeFormat: String? = null                // 时间格式
        var partitionKey: String? = null                // 分区键
        var partitionValue: String? = null              // 分区键值
        var whereCondition: String? = null              // where条件的值


        // 2. > 导出 export
        var exportHiveDataBase: String? = null          // hive的数据库
        var exportHiveTable: String? = null             // hive的表
        var exportKey: List<String>? = null             // 导出数据源的唯一键(列)
        // 2.1 >> 全量 full

        // 2.2 >> 增量 increment
        var exportFixedPartition: Boolean? = null       // 固定分区模式是否开启
        var exportPartitionKey: String? = null          // 增量的分区字段
        var exportPartitionValue: String? = null        // 增量的分区字段
        var exportPartitionKeyValue: String? = null     // 固定分区键值对
        var exportColumns: List<String>? = null         // 指定要导出到的columns
        // var timeOffset: Int? = null                  // 时间偏移量, 同上


        companion object {
            fun build(type: Int, host: String, port: Int, url: String, dataBase: String, tableName: String): DataSource {
                val d = DataSource()
                d.type = type
                d.host = host
                d.port = port
                d.url = url
                d.dataBase = dataBase
                d.tableName = tableName
                return d
            }
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other?.javaClass != javaClass) return false

            other as DataSource

            if (type != other.type) return false
            if (host != other.host) return false
            if (port != other.port) return false
            if (url != other.url) return false
            if (dataBase != other.dataBase) return false
            // if (tableName != other.tableName) return false // 按前面几个字段进行分组

            return true
        }

        override fun hashCode(): Int {
            var result = type ?: 0
            result = 31 * result + (host?.hashCode() ?: 0)
            result = 31 * result + (port?.hashCode() ?: 0)
            result = 31 * result + (url?.hashCode() ?: 0)
            result = 31 * result + (dataBase?.hashCode() ?: 0)
            // result = 31 * result + (tableName?.hashCode() ?: 0)  // 按前面几个字段进行分组
            return result
        }
    }
}