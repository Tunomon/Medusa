package upc.edu.medusa.oozie.model

import com.avaje.ebean.ExpressionList
import com.avaje.ebean.Model
import java.io.Serializable
import com.avaje.ebean.annotation.EnumValue
import com.fasterxml.jackson.core.type.TypeReference
import upc.edu.medusa.common.JsonMapper
import upc.edu.medusa.common.util.Splitters
import upc.edu.medusa.common.util.XmlHelper
import java.util.*
import javax.persistence.Entity
import javax.persistence.Table

/**
 * Created by msi on 2017/5/9.
 */
@Entity
@Table(name = "medusa_oozie_jobs")
open class OozieJob : Serializable {

    var jobId: String? = null       // 对应Oozie的jobId
    var template: String? = null    // 模板的对象信息
    // var templateId: Long? = null    // oozie template id
    var name: String? = null        // 对应Oozie的app-name
    var path: String? = null        // 对应Oozie的app-path
    var type: Int? = null           // Job类型
    var status: String? = null      // Job的状态
    var jobFrom: Int? = null        // job的生成方式, 1: 模板, 2: SQL
    var definition: String? = null  // Job的定义文件
    // var resource: String? = null    // json视图文件
    // var analyzeMode: Int? = null    // oozie job解析模式，1: 视图模式(JSON), 2: 文本模式(XML) @see OozieJobTemplate.AnalyzeMode
    var conf: String? = null        // 提交job的配置信息, xml格式
    var userName: String? = null    // 提交的用户名称
    var groupName: String? = null   // 提交的组名称
    var log: String? = null         // Oozie 记录的日志
    var notifyUrl: String? = null   // 回调Url
    var startAt: Date? = null       // 开始时间
    var endAt: Date? = null         // 结束时间
    var modifiedAt: Date? = null    // 最后一次修改时间
    var creatorId: Long? = null     // 创建人id
    var creatorName: String? = null // 创建人姓名
    var updatorId: Long? = null     // 更新人id
    var updatorName: String? = null // 更新人姓名

    //企业信息
    var orgId: Long? = null

    @Transient
    var process: Int? = null        // 进度条值
    @Transient
    var jobDesc: String? = null     // job描述

    /**
     * @see WorkflowJob
     */
    var extra: String? = null       // extra 字段

    enum class Type(val key: Int, val desc: String) {
        @EnumValue("1") WORKFLOW(1, "WORKFLOW"),
        @EnumValue("2") COORDINATOR(2, "COORDINATOR"),
        @EnumValue("3") BUNDLE(3, "BUNDLE")
    }
    enum class JobFrom(val key: Int, val desc: String) {
        @EnumValue("1") TEMPLATE(1, "TEMPLATE"),
        @EnumValue("2") SQL(2, "SQL"),
        @EnumValue("3") DATASOURCE(3, "DATASOURCE")
    }

    companion object {
        val find = object : Model.Find<Long, OozieJob>() {}

        // 构建查询对象
        fun buildQuery(criterias: Map<String, Any>): ExpressionList<OozieJob> {
            val where = OozieJob.find.where()
            // 构建参数
            setParam(where, criterias, "jobId", "jobId")
            setParam(where, criterias, "name", "name")
            setParam(where, criterias, "jobType", "type")
            setParam(where, criterias, "status", "status")
            setParam(where, criterias, "userName", "userName")
            setParam(where, criterias, "groupName", "groupName")
            setParam(where, criterias, "creatorId", "creatorId")
            setParam(where, criterias, "creatorName", "creatorId")
            setParam(where, criterias, "updatorId", "updatorId")
            setParam(where, criterias, "updatorName", "updatorName")
            setParam(where, criterias, "userId", "creatorId")
            setParam(where, criterias, "orgId", "orgId")

            // 多值参数
            setParams(where, criterias, "queryType", "status")
            setParams(where, criterias, "category", "status")
            setParams(where, criterias, "userIds", "creatorId")

            // like参数
            setParamLike(where, criterias, "searchName", "name")

            // 大于等于的参数
            setParamGE(where, criterias, "createdStart", "createdAt")

            // 小于的参数
            setParamLE(where, criterias, "createdEnd", "createdAt")

            // 默认 created_at 降序
            where.order().desc("createdAt")
            return where
        }

        private fun setParam(where: ExpressionList<OozieJob>, criterias: Map<String, Any>, param: String, column: String) {
            val value = criterias[param]
            if (value != null) {
                if (value is String && value.isBlank())
                    return
                where.eq(column, value)
            }
        }

        private fun setParams(where: ExpressionList<OozieJob>, criterias: Map<String, Any>, param: String, column: String) {
            val values = criterias[param]
            if (values != null) {
                if (values is String && values.isBlank())
                    return
                where.`in`(column, Splitters.COMMA.splitToList(values.toString()))
            }
        }

        private fun setParamLike(where: ExpressionList<OozieJob>, criterias: Map<String, Any>, param: String, column: String) {
            val value = criterias[param]
            if (value != null) {
                if (value is String && value.isBlank())
                    return
                where.like(column, "%$value%")
            }
        }

        private fun setParamGE(where: ExpressionList<OozieJob>, criterias: Map<String, Any>, param: String, column: String) {
            val value = criterias[param]
            if (value != null) {
                if (value is String && value.isBlank())
                    return
                where.ge(column, value)
            }
        }

        private fun setParamLE(where: ExpressionList<OozieJob>, criterias: Map<String, Any>, param: String, column: String) {
            val value = criterias[param]
            if (value != null) {
                if (value is String && value.isBlank())
                    return
                where.le(column, value)
            }
        }
    }

    /**
     * 设置 extra 参数值
     */
    fun setExtras(vararg pairs: Pair<String, Any?>) {
        val extraMap: HashMap<String, Any?> = getExtraMap()
        for ((key, value) in pairs) {
            extraMap[key] = value
        }
        extra = JsonMapper.JSON_NON_DEFAULT_MAPPER.toJson(extraMap)
    }

    /**
     * 获取 extra 参数值
     */
    fun getExtra(key: String): Any? {
        return getExtraMap()[key]
    }

    /**
     * 获取 extra Map
     */
    fun getExtraMap(): HashMap<String, Any?> {
        val extraMap: HashMap<String, Any?> = hashMapOf()
        if (!extra.isNullOrBlank()) {
            extraMap.putAll(JsonMapper.JSON_NON_DEFAULT_MAPPER.mapper.readValue(extra, object : TypeReference<Map<String, Any>>() {}))
        }
        return extraMap
    }

    // 判断是否是 workflow
    fun isWorkflowJob(): Boolean {
        return this.type == Type.WORKFLOW.key
    }

    // 判断是否是 Coordinator
    fun isCoordinatorJob(): Boolean {
        return this.type == Type.COORDINATOR.key
    }

    // 判断是否是 Bundle
    fun isBundleJob(): Boolean {
        return this.type == Type.BUNDLE.key
    }

    // 解析 configuration
    fun getConfiguration(): MutableMap<String, String> {
        val configurations: MutableMap<String, String> = mutableMapOf()
        if (!conf.isNullOrBlank()) {
            val document = XmlHelper.toDocument(conf!!.byteInputStream())
            val node = XmlHelper.getNode("/configuration", document)
            XmlHelper.getChildrenNodes(node, "property").forEach {
                val name = XmlHelper.getChildrenSingleNode(it, "name")
                val value = XmlHelper.getChildrenSingleNode(it, "value")

                // 特殊处理: 过滤 oozie 回调
                if (!value.textContent.contains("/api/horus/oozie/jobs/notify")) {
                    configurations[name.textContent] = value.textContent
                }
            }
        }
        return configurations
    }

}