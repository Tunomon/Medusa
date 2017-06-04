package upc.edu.medusa.oozie.model

import com.avaje.ebean.ExpressionList
import com.avaje.ebean.Model
import upc.edu.medusa.common.JsonMapper
import upc.edu.medusa.common.util.Splitters
import java.io.Serializable
import javax.persistence.Entity
import javax.persistence.Table

/**
 * Created by msi on 2017/5/9.
 */
@Entity
@Table(name = "medusa_oozie_job_templates")
open class OozieJobTemplate  :  Serializable {
    // job 模板别名, 不能含有中文, 正则: ([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39}
    var jobName: String? = null

    // 前台传递Json(对象提交方式), 便于反显
    var webJson: String? = null

    // 前台传递xml(xml 文本提交方式)
    var webXml: String? = null

    // sql解析, 封装的sql
    var webSql: String? = null

    // 解析成的 job json
    var jobJson: String? = null

    /**
     * job模式: (视图模式 和 Xml 文本模式)
     * @see AnalyzeMode
     */
    var analyzeMode: Int? = null

    /**
     * job 类型
     * @see OozieJob.Type
     */
    var jobType: Int? = null

    // 模板描述
    var jobDesc: String? = null

    var creatorId: Long? = null
    var creatorName: String? = null
    var updatorId: Long? = null
    var updatorName: String? = null

    //企业信息
    var orgId: Long? = null

    // 定时任务的前台额外配置参数
    var coordExtra: String? = null

    // 是否立即执行, 前台参数, 默认为 false
    @Transient
    var execute: Boolean = false

    // 模板的生成来源, @see OozieJob.JobFrom
    @Transient
    var from: Int? = null

    /**
     * 解析模式
     */
    enum class AnalyzeMode(val key: Int, val desc: String) {
        VIEW(1, "视图模式(JSON)"),
        XML(2, "文本模式(XML)"),
        SQL(3, "sql解析模式(SQL)"),
        DATASOURCE(4, "datasource解析模式(DATASOURCE)")
    }

//    // 获取 coordinator 的参数
//    fun getCoordParams(): CoordParams {
//        if (coordExtra.isNullOrBlank()) {
//            return CoordParams.build()
//        }
//        return JsonMapper.JSON_NON_DEFAULT_MAPPER.fromJson(coordExtra, CoordParams::class.java)
//    }

    //构建查询对象
    companion object {
        val find = object : Model.Find<Long, OozieJobTemplate>() {}
        fun buildQuery(criterias: Map<String, Any?>): ExpressionList<OozieJobTemplate> {
            val where = OozieJobTemplate.find.where()
            //构建参数
            setParamLike(where, criterias, "jobName", "jobName")
            setParams(where, criterias, "userId", "creatorId")
            setParam(where, criterias, "orgId", "orgId")
            //默认按照创建时间降序
            where.order().desc("createdAt")
            return where
        }

        private fun setParams(where: ExpressionList<OozieJobTemplate>?, criterias: Map<String, Any?>, param: String, column: String) {
            val values = criterias[param]
            if (values != null) {
                if (values is String && values.isBlank())
                    return
                where!!.`in`(column, Splitters.COMMA.splitToList(values.toString()))
            }

        }

        private fun setParam(where: ExpressionList<OozieJobTemplate>, criterias: Map<String, Any?>, param: String, column: String) {
            val value = criterias[param]
            if (value != null) {
                if (value is String && value.isBlank())
                    return
                where.eq(column, value)
            }
        }

        private fun setParamLike(where: ExpressionList<OozieJobTemplate>?, criterias: Map<String, Any?>, param: String, column: String) {
            val value = criterias[param]
            if (value != null) {
                if (value is String && value.isBlank()) return
                where!!.like(column, "%$value%")
            }
        }
    }
}