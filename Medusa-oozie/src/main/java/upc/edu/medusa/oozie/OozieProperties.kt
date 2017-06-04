package upc.edu.medusa.oozie

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Created by msi on 2017/5/9.
 */
@ConfigurationProperties(prefix = "oozie")
class OozieProperties {

    /**
     * Oozie 服务地址
     */
    var site: String? = null

    /**
     * Oozie 任务的提交者, 如果没有配置则获取oozie的用户
     */
    var user: String? = null

    /**
     * oozie launcher 任务提交队列
     * 默认 oozie 队列
     */
    var launcher: String = "oozie"

    /**
     * Oozie 的 userName (oozie的默认用户)
     */
    var userName: String? = null

    /**
     * hdfs 地址
     */
    var hdfs: String? = null

    /**
     * job tracker/resource manager 地址
     */
    var jobTracker: String? = null

    /**
     * hive jdbc url
     */
    var hiveJdbc: String? = null

    /**
     * hive-site url
     */
    var jobXml: String? = null

    /**
     * spark url
     */
    var spark: String? = null

    /**
     * Oozie Server Share Lib Path
     */
    var libSharePath: String? = null

    /**
     * Oozie Server 自定义 Share Lib Path, 位于share目录下
     */
    var libCustomPath: String? = null

    /**
     * oozie 回调 url 地址, 不包含回调参数
     * 参数在 OozieJobManager 进行拼接
     */
    var notify: String? = null

    /**
     * 时区, Oozie默认为UTC
     */
    var GMT: String = "+0800"
}