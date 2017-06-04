package upc.edu.medusa.oozie.dto

/**
 * Created by msi on 2017/5/11.
 */
class DefaultProperty {

    /**
     * hive jdbc url
     */
    var hiveJdbc: String? = null

    /**
     * spark url
     */
    var spark: String? = null

    /**
     * hive-site url
     */
    var jobXml: String? = null

    // 任务提交者
    var user: String? = null
    // oozie 默认的任务提交者
    var userName: String? = null
}