package upc.edu.medusa.hadoop.model

import org.apache.hadoop.fs.FileStatus
import upc.edu.medusa.hadoop.service.toDate
import java.io.Serializable
import java.text.DecimalFormat
import java.util.*

/**
 * Created by msi on 2017/5/8.
 */
open class HdfsFile : Serializable {

    var path: String? = null          // 当前文件路径
    var currentDir: String? = null    // 当前所在目录
    var name: String? = null          // 文件名称
    var type: Int? = null             // 文件类型(文件、文件夹)
    var suffix: String? = null        // 文件后缀
    var originalSize: Long? = null    // 文件原始大小
    var size: String? = null          // 文件大小
    var unit: String? = null          // 文件大小的单位
    var owner: String? = null         // 文件所属用户
    var group: String? = null         // 文件所属组
    var permission: String? = null    // 文件权限
    var modifyDate: Date? = null      // 修改时间
    var replication: Int? = null      // 副本数

    /**
     * 文件类型
     */
    enum class Type(val key: Int, val desc: String) {
        DIR(1, "目录"),
        FILE(2, "文件")
    }

    companion object {
        fun build(fileStatus: FileStatus): HdfsFile {
            val hdfsFile = HdfsFile()
            hdfsFile.path = fileStatus.path.toString()
            hdfsFile.name = fileStatus.path.name
            hdfsFile.currentDir = fileStatus.path.parent.toUri().path
            hdfsFile.owner = fileStatus.owner
            hdfsFile.group = fileStatus.group
            hdfsFile.modifyDate = fileStatus.modificationTime.toDate()
            if (fileStatus.isFile) {
                // 计算后缀
                val dot = fileStatus.path.name.lastIndexOf(".")
                if (dot > 0) {
                    hdfsFile.suffix = fileStatus.path.name.substring(dot + 1)
                }
                hdfsFile.type = HdfsFile.Type.FILE.key
                hdfsFile.permission = "-${fileStatus.permission}"
                hdfsFile.replication = fileStatus.replication.toInt()
                hdfsFile.originalSize = fileStatus.len
                // 计算文件大小
                val pair = convertFileSize(fileStatus.len)
                hdfsFile.size = pair.component1()
                hdfsFile.unit = pair.component2()
            }
            if (fileStatus.isDirectory) {
                hdfsFile.type = HdfsFile.Type.DIR.key
                hdfsFile.permission = "d${fileStatus.permission}"
                hdfsFile.replication = 0
            }
            return hdfsFile
        }

        /**
         * 转换Long类型文件大小, key为大小, value为单位(B, KB, MB, GB, TB)
         */
        private fun convertFileSize(size: Long): Pair<String, String> {
            if (size <= 0) {
                return Pair("0", "B")
            }
            val df = DecimalFormat("0.00")
            val kb: Long = 1024
            val mb: Long = kb * 1024
            val gb: Long = mb * 1024
            val tb: Long = gb * 1024

            if (tb <= size) {
                return Pair(df.format(size.div(tb.toDouble())), "TB")
            }
            else if (gb <= size && size < tb) {
                return Pair(df.format(size.div(gb.toDouble())), "GB")
            }
            else if (mb <= size && size < gb) {
                return Pair(df.format(size.div(mb.toDouble())), "MB")
            }
            else if (kb <= size && size < mb) {
                return Pair(df.format(size.div(kb.toDouble())), "KB")
            }
            else {
                return Pair("$size", "B")
            }
        }
    }

    fun isIfFile(): Boolean {
        return Type.FILE.key == type
    }

    fun isIfDir(): Boolean {
        return Type.DIR.key == type
    }

    override fun toString(): String {
        return "HdfsFile[path=$path, currentDir=$currentDir, name=$name, type=$type, suffix=$suffix, size=$size, unit=$unit, " +
                "owner=$owner, group=$group, permission=$permission, modifyDate=$modifyDate, replication=$replication]"
    }
}