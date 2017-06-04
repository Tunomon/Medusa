package upc.edu.medusa.query.model

import com.avaje.ebean.Model
import com.fasterxml.jackson.databind.ObjectMapper
import upc.edu.medusa.common.JsonMapper
import java.text.DecimalFormat
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table

/**
 * Created by msi on 2017/5/14.
 */
@Entity
@Table(name = "medusa_table_info")
class MedusaTableInfo {
    companion object {
        val find = object : Model.Find<Long, MedusaTableInfo>() {}

        val OBJECT_MAPPER: ObjectMapper = JsonMapper.JSON_NON_DEFAULT_MAPPER.mapper

        fun convertSize(size: Long): Pair<String, String> {
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
            } else if (size in gb..(tb - 1)) {
                return Pair(df.format(size.div(gb.toDouble())), "GB")
            } else if (size in mb..(gb - 1)) {
                return Pair(df.format(size.div(mb.toDouble())), "MB")
            } else if (size in kb..(mb - 1)) {
                return Pair(df.format(size.div(kb.toDouble())), "KB")
            } else {
                return Pair("$size", "B")
            }
        }
    }

    //id
    @Id
    var id: Long? = null
    //表名称
    var name: String? = null

    //所属数据库
    var databaseBelongs: String? = null

    //占用存储空间(byte)
    var storageSpace: Long? = null

    //占用空间大小(给前台的大小,需要进行格式化为两位小数)
    @Transient
    var size: String? = null

    //占用存储空间单位
    @Transient
    var unit: String? = null

    //数据描述
    var tableDesc: String? = null

    //是否含有分区信息
    var isPartition: Boolean? = null

    //是否是扩展表
    var isExternal: Boolean? = null

    //文件存储位置
    var location: String? = null


}