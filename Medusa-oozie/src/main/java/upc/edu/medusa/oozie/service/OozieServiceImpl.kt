package upc.edu.medusa.oozie.service

import org.apache.commons.lang.math.RandomUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import upc.edu.medusa.common.JsonMapper
import upc.edu.medusa.oozie.OozieProperties
import upc.edu.medusa.oozie.manager.HDFSManager
import upc.edu.medusa.oozie.manager.OozieJobManager
import upc.edu.medusa.oozie.model.OozieJob
import upc.edu.medusa.oozie.model.OozieJobTemplate
import upc.edu.medusa.oozie.operator.OoziePropertieManager
import upc.edu.medusa.oozie.xml.action.Sqoop
import upc.edu.medusa.oozie.xml.app.WorkflowApp
import upc.edu.medusa.oozie.xml.node.Global
import java.util.*

/**
 * Created by msi on 2017/5/9.
 */
@Service
class OozieServiceImpl : OozieService{



    @Autowired
    lateinit var oozieProperties: OozieProperties

    @Autowired
    lateinit var oozieJobManager: OozieJobManager

    @Autowired
    lateinit var hdfsManager: HDFSManager

    @Autowired
    lateinit var ooziePropertieManager: OoziePropertieManager

    @Autowired
    lateinit var oozieJobService: OozieJobService

    override fun submitWorkflow(workflowApp: WorkflowApp, jobProperties: Properties): Boolean {
        // 储存时将xml解析为json，然后储存为template的jobJson内，使用时解析出来
        // val workflowApp = JsonMapper.JSON_NON_DEFAULT_MAPPER.fromJson(template.jobJson, WorkflowApp::class.java)


        // 3. 生成 Wf oozie工作目录, 并设置运行时属性(job.properties)
        val applicationPath = createWFOozieWorkspaces(workflowApp, jobProperties)

        // 4. 提交job, (是否运行)
        val jobId = oozieJobManager.submitWorkflow(applicationPath, jobProperties)

         oozieJobManager.start(jobId)
//        return oozieJobService.synchronizeWorkflow(jobId, template, null, creatorId, creatorName, creatorId, creatorName, orgId)
        return true
    }


//    override fun start(id: Long, userId: Long, userName: String): Boolean {
//    }



    /**
     * 生成 WF Oozie 的工作目录, 返回应用的路径
     */
    private fun createWFOozieWorkspaces(workflowApp: WorkflowApp, jobProperties: Properties): String {
        // important --> 设置 oozie 的 launcher 队列
        val global = Global()
        global.configuration["oozie.launcher.mapred.job.queue.name"] = oozieProperties.launcher
        workflowApp.global = global

        // 创建hdfs目录和lib目录
        val suffix = "${System.currentTimeMillis()}${RandomUtils.nextInt(1000)}" // 当前毫秒数 + 随机数
        val hdfsJobDir = "${ooziePropertieManager.getOozieWorkPath()}/${workflowApp.name}/${workflowApp.xmlLabel}$suffix"
        val hdfslibDir = "${ooziePropertieManager.getOozieWorkPath()}/${workflowApp.name}/${workflowApp.xmlLabel}$suffix/lib"
        if (!hdfsManager.exist(hdfsJobDir)) {
            hdfsManager.createDir(hdfsJobDir)
        }
        if (!hdfsManager.exist(hdfslibDir)) {
            hdfsManager.createDir(hdfslibDir)
        }

        // 拷贝相关文件
        workflowApp.actions.forEach {

            // sqoop job
            val sqoop = it.sqoop
            if (sqoop != null) {
                jobProperties[Sqoop.OOZIE_USE_SYSTEM_LIBPATH] = "true"
//                jobProperties[Sqoop.OOZIE_LIBPATH] = oozieJobManager.libCustomPathOnHDFS()
                jobProperties[Sqoop.OOZIE_LIBPATH] = "hdfs://192.168.1.130:9000/user/fan/lib"
            }

        }

        // 创建workflow.xml
        val appPath = "$hdfsJobDir/workflow.xml"
        hdfsManager.createFile(appPath, workflowApp.toXML().byteInputStream(), true)

        return appPath
    }




}