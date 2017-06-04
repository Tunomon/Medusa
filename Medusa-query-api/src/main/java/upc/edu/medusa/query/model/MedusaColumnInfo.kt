package upc.edu.medusa.query.model

import com.avaje.ebean.Model
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table

/**
 * Created by msi on 2017/4/19.
 */
@Entity
@Table(name = "medusa_column_info")
class MedusaColumnInfo {

    companion object {
        val find = object : Model.Find<Long, MedusaColumnInfo>() {}
    }

    @Id
    var id: Long? = null

    //数据表ID
    var tid: Long? = null

    //字段名称
    var name: String = ""

    //字段类型
    var type: String? = null

    //数据描述
    @Column(name = "`desc`")
    var desc: String? = null

    //额外信息
    var extra: String? = null
}