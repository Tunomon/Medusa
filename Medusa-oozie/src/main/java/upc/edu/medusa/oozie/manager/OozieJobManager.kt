package upc.edu.medusa.oozie.manager

import com.google.common.base.Throwables
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.oozie.client.CoordinatorAction
import org.apache.oozie.client.CoordinatorJob
import org.apache.oozie.client.WorkflowJob
import org.apache.oozie.client.XOozieClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import upc.edu.medusa.oozie.OozieProperties
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.net.URI
import java.util.*
import javax.annotation.PostConstruct

/**
 * Created by msi on 2017/5/11.
 */
@Component
open class OozieJobManager {
    @Autowired
    lateinit var xOozieClient: XOozieClient
    @Autowired
    lateinit var oozieProperties: OozieProperties
    @Autowired
    lateinit var configuration: Configuration

    /**
     * 提交一个workflow作业
     */
    fun submitWorkflow(applicationPath: String, jobProperties: Properties): String {
        val config = xOozieClient.createConfiguration()
        config.putAll(jobProperties)

        // username
        if (oozieProperties.user.isNullOrBlank()) {
            config["user.name"] = oozieProperties.userName
        } else {
            config["user.name"] = oozieProperties.user
        }

        // workflow app path
        config[XOozieClient.APP_PATH] = applicationPath

        // 回调 url 设置
        config[XOozieClient.WORKFLOW_NOTIFICATION_URL] = notifyWorkflow()
        config[XOozieClient.ACTION_NOTIFICATION_URL] = notifyWorkflowAction()
        return xOozieClient.submit(config)
    }

    /**
     * 提交一个 coordinator 作业
     */
    fun submitCoordinator(applicationPath: String, jobProperties: Properties): String {
        val config = xOozieClient.createConfiguration()
        config.putAll(jobProperties)

        // username
        if (oozieProperties.user.isNullOrBlank()) {
            config["user.name"] = oozieProperties.userName
        } else {
            config["user.name"] = oozieProperties.user
        }

        // coordinator app path
        config[XOozieClient.COORDINATOR_APP_PATH] = applicationPath

        // 回调 workflow url 设置
        config[XOozieClient.WORKFLOW_NOTIFICATION_URL] = notifyWorkflow()
        config[XOozieClient.ACTION_NOTIFICATION_URL] = notifyWorkflowAction()
        // 回调 coordinator url 设置
        config["oozie.coord.workflow.notification.url"] = notifyCoordinatorAction()
        config[XOozieClient.COORD_ACTION_NOTIFICATION_URL] = notifyCoordinatorAction() // action 回调参数疑问?
        return xOozieClient.submit(config)
    }

    /**
     * 根据JobId, 开始一个作业 / workflow
     */
    fun start(jobId: String) {
        if (jobId.isNullOrBlank()) {
            throw Exception("oozie server job id can not be null or blank")
        }
        // 校验job的是否存在
        xOozieClient.getJobInfo(jobId) ?: throw IllegalArgumentException("oozie server this job in oozie is not exist")

        // start job
        xOozieClient.start(jobId)
    }

    /**
     * 根据jobId, rerun 一个job / workflow
     */
    fun rerun(jobId: String, configuration: Properties) {
        if (jobId.isNullOrBlank()) {
            throw Exception("oozie server job id can not be null or blank")
        }
        // 校验job的是否存在
        xOozieClient.getJobInfo(jobId) ?: throw IllegalArgumentException("oozie server this job in oozie is not exist")

        // reRun job
        // TODO: 设置跳过的节点, 默认无
        configuration["oozie.wf.rerun.skip.nodes"] = ","
        xOozieClient.reRun(jobId, configuration)
    }

    /**
     * 根据jobId, rerun 一个job / coordinator
     */
    fun rerunCoord(jobId: String, rerunType: String, scope: String) {
        if (jobId.isNullOrBlank()) {
            throw Exception("oozie server job id can not be null or blank")
        }
        // 校验job的是否存在
        xOozieClient.getJobInfo(jobId) ?: throw IllegalArgumentException("oozie server this job in oozie is not exist")

        xOozieClient.reRunCoord(jobId, rerunType, scope, false, false)
    }

    /**
     * 根据jobId, kill 一个job  /workflow/coordinator/bundle
     */
    fun kill(jobId: String) {
        if (jobId.isNullOrBlank()) {
            throw Exception("oozie server job id can not be null or blank")
        }
        // 校验job的是否存在
        xOozieClient.getJobInfo(jobId) ?: throw IllegalArgumentException("oozie server this job in oozie is not exist")

        // kill
        xOozieClient.kill(jobId)
    }

    /**
     * 根据jobId, suspend 一个job
     */
    fun suspend(jobId: String) {
        if (jobId.isNullOrBlank()) {
            throw Exception("oozie server job id can not be null or blank")
        }
        // 校验job的是否存在
        xOozieClient.getJobInfo(jobId) ?: throw IllegalArgumentException("oozie server this job in oozie is not exist")

        xOozieClient.suspend(jobId)
    }

    /**
     * 根据jobId, resume 一个job
     */
    fun resume(jobId: String) {
        if (jobId.isNullOrBlank()) {
            throw Exception("oozie server job id can not be null or blank")
        }
        // 校验job的是否存在
        xOozieClient.getJobInfo(jobId) ?: throw IllegalArgumentException("oozie server this job in oozie is not exist")

        xOozieClient.resume(jobId)
    }

    /**
     *  获取 workflow oozie job 信息
     */
    fun workflowJobInfo(jobId: String): WorkflowJob = xOozieClient.getJobInfo(jobId)

    /**
     *  获取 coordinator oozie job 信息
     */
    fun coordinatorJobInfo(jobId: String): CoordinatorJob = xOozieClient.getCoordJobInfo(jobId)

    /**
     * 获取 coordinator 的 action info
     */
    fun coordinatorActionInfo(actionId: String): CoordinatorAction = xOozieClient.getCoordActionInfo(actionId)

    /**
     * 获取 job 的定义
     */
    fun definition(jobId: String): String = xOozieClient.getJobDefinition(jobId)

    /**
     * 获取 job 的 log
     * logLevel: ALL, DEBUG, ERROR, INFO, TRACE, WARN, FATAL.
     * recent: h or m , such as 24h or 30m
     */
    fun log(jobId: String, logLevel: String, recent: String): String {
        val buffer = ByteArrayOutputStream()
        // 获取 WARN 类型的log, 并获取前一周时间
        xOozieClient.getJobLog(jobId, null, null, "loglevel=$logLevel;recent=$recent", PrintStream(buffer))
        return buffer.toString("UTF-8")
    }

    /**
     * 获取oozie服务的用户名
     */
    fun userName(): String? {
        // val httpRequest = HttpRequest.get("${oozieProperties.site}/v1/admin/java-sys-properties")
        // val properties = JsonMapper.JSON_NON_DEFAULT_MAPPER.fromJson(httpRequest.body(), Map::class.java)
        // val userName = properties["user.name"] ?: throw ServiceException("oozie server user name is null !!!")
        val userName = xOozieClient.javaSystemProperties["user.name"] ?: throw IllegalArgumentException("oozie server user name is null !!!")
        return userName.toString()
    }

    /**
     * 获取整个 lib path 在 hdfs 上的目录
     */
    fun libPathOnHDFS(): String? {
        if (oozieProperties.libSharePath.isNullOrBlank()) {
            initOozieJobManager()
        }
        // share path + custom share path
        return "${oozieProperties.hdfs}${oozieProperties.libSharePath},${oozieProperties.hdfs}${oozieProperties.libCustomPath}"
    }

    /**
     * 获取整个 lib share path 在 hdfs 上的目录
     */
    fun libSharePathOnHDFS(): String? {
        if (oozieProperties.libSharePath.isNullOrBlank()) {
            initOozieJobManager()
        }
        // share path + custom share path
        return "${oozieProperties.hdfs}${oozieProperties.libSharePath}"
    }

    /**
     * 获取整个 lib custom path 在 hdfs 上的目录
     */
    fun libCustomPathOnHDFS(): String? {
        if (oozieProperties.libCustomPath.isNullOrBlank()) {
            initOozieJobManager()
        }
        // share path + custom share path
        return "${oozieProperties.hdfs}${oozieProperties.libCustomPath}"
    }

    /**
     *  获取 oozie lib share 的
     *  注意: 不包括 hdfs 协议头, 如 /user/admin/share/lib
     */
    private fun libSharePath(): String? {
        // val httpRequest = HttpRequest.get("${oozieProperties.site}/v1/admin/configuration")
        // val properties = JsonMapper.JSON_NON_DEFAULT_MAPPER.fromJson(httpRequest.body(), Map::class.java)
        // val libPath = properties["oozie.service.WorkflowAppService.system.libpath"] ?: throw ServiceException("oozie server share lib path is null !!!")
        val libPath = xOozieClient.serverConfiguration["oozie.service.WorkflowAppService.system.libpath"] ?: throw IllegalArgumentException("oozie server share lib path is null !!!")
        return libPath.toString()
    }

    /**
     * 获取自定义的 oozie lib share 路径
     * 如果 share lib 为 /user/admin/share/lib
     * 那么 custom lib 为 /user/admin/share/custom
     *
     */
    private fun libCustomPath(): String? {
        val libSharePath = libSharePath()
        if (libSharePath!!.endsWith("/lib")) {
            return libSharePath.substring(0, libSharePath.length - 3) + "custom"
        }
        return ""
    }

    /**
     * 校验Oozie server 是否可以访问
     */
    fun isHealth(): Boolean {
        try {
            val systemMode = xOozieClient.systemMode
            return systemMode != null
        } catch (e: Exception) {
            return false
        }
    }

    /**
     * 获取 workflow 的回调 url
     * 当 oozie job 的状态发生改变的时候, 进行回调
     */
    fun notifyWorkflow(): String = "${oozieProperties.notify}?jobId=\$jobId&status=\$status"

    /**
     * 获取 coordinator action 的回调 url
     * 当 oozie job 的状态发生改变的时候, 进行回调
     */
    fun notifyCoordinatorAction(): String = "${oozieProperties.notify}?actionId=\$actionId&status=\$status"

    /**
     * 获取 workflow actions 的回调 url
     * 当 oozie job 每次进入或退出Action的时候, 进行回调
     *
     */
    fun notifyWorkflowAction(): String = "${oozieProperties.notify}?jobId=\$jobId&status=\$status&nodeName=\$nodeName"

    @PostConstruct
    fun initOozieJobManager() {
        try {
            if (!isHealth()) {
                throw IllegalArgumentException("oozie server is not available !!!")
            }
            // 用户名称
            oozieProperties.userName = userName()

            // share lib path
            oozieProperties.libSharePath = libSharePath()

            // 自定义 share lib path
            oozieProperties.libCustomPath = libCustomPath()

        } catch (e: Exception) {
            throw e
        }

        // filesystem
        try {
            val fileSystem = FileSystem.get(URI(oozieProperties.hdfs), configuration, oozieProperties.userName)
            HDFSManager.fileSystem = fileSystem

            if (oozieProperties.jobXml.isNullOrBlank() || !fileSystem!!.exists(Path(oozieProperties.jobXml))) {
                throw IllegalArgumentException("hive-site.xml not exists, please place the file in advance")
            }
        } catch (e: Exception) {
            System.out.println("hdfs fileSystem init failed, cause by ${Throwables.getStackTraceAsString(e)}")
        }
    }
}