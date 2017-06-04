package upc.edu.medusa.oozie.service

import com.google.common.base.Throwables
import org.apache.oozie.client.XOozieClient
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import upc.edu.medusa.common.JsonMapper
import upc.edu.medusa.oozie.OozieProperties
import upc.edu.medusa.oozie.manager.OozieJobManager
import upc.edu.medusa.oozie.model.OozieJob
import upc.edu.medusa.oozie.model.OozieJobTemplate
import upc.edu.medusa.oozie.utils.secondsBetween

/**
 * Created by msi on 2017/5/11.
 */
@Service
class OozieJobServiceImpl : OozieJobService {

    @Autowired
    lateinit var oozieJobManager: OozieJobManager

    @Autowired
    lateinit var oozieProperties: OozieProperties

    override fun synchronizeWorkflow(oozieJobId: String, template: OozieJobTemplate, nodeName: String?, creatorId: Long?, creatorName: String?, updatorId: Long?, updatorName: String?, orgId: Long?): OozieJob {
        try {
            val workflowJobInfo = oozieJobManager.workflowJobInfo(oozieJobId)
            val oozieJob: OozieJob = OozieJob()

            // 塞入job相关的数据, 以及extra参数
            oozieJob.jobId = workflowJobInfo.id
            oozieJob.template = JsonMapper.JSON_NON_DEFAULT_MAPPER.toJson(template)
            // oozieJob.name = workflowJobInfo.appName // 会乱码
            // oozieJob.templateId = templateId
            oozieJob.name = template.jobName
            oozieJob.conf = workflowJobInfo.conf
            // oozieJob.path = workflowJobInfo.appPath // 莫名其妙的乱码
            oozieJob.path = oozieJob.getConfiguration()[XOozieClient.APP_PATH]
            oozieJob.type = OozieJob.Type.WORKFLOW.key
            oozieJob.status = workflowJobInfo.status.name
            oozieJob.jobFrom = template.from
            oozieJob.definition = oozieJobManager.definition(oozieJobId)
            oozieJob.log = oozieJobManager.log(oozieJobId, "WARN", "168h")  // 默认获取前一周的日志, 日志级别为WARN
            // oozieJob.resource = frontResource
            // oozieJob.analyzeMode = analyzeMode
            oozieJob.notifyUrl = oozieProperties.notify
            oozieJob.userName = workflowJobInfo.user
            oozieJob.groupName = workflowJobInfo.acl
            oozieJob.startAt = workflowJobInfo.startTime
            oozieJob.endAt = workflowJobInfo.endTime
            oozieJob.modifiedAt = workflowJobInfo.lastModifiedTime
            oozieJob.creatorId = creatorId
            oozieJob.creatorName = creatorName
            oozieJob.updatorId = updatorId
            oozieJob.updatorName = updatorName
            oozieJob.orgId = orgId
            oozieJob.setExtras(
                    "externalId" to workflowJobInfo.externalId,
                    "duration" to workflowJobInfo.startTime.secondsBetween(workflowJobInfo.endTime),
                    "run" to workflowJobInfo.run,
                    "parentId" to workflowJobInfo.parentId
            )
//            val workflowJob = PaasRespHelper.orServEx(findJobByJobId(oozieJobId))
//            if (workflowJob == null) {
//                createJob(oozieJob)
//            } else {
//                oozieJob.id = workflowJob.id
//                updateJob(oozieJob)
//            }
//
//            // 同步 Action 相关
//            workflowJobInfo.actions/*.filter {
//                if (nodeName.isNullOrBlank()) {
//                    true
//                } else {
//                    it.name.equals(nodeName)
//                }
//            }*/.forEach { action ->
//
//                val oozieAction: OozieWorkflowAction = OozieWorkflowAction.build(action, workflowJobInfo.id)
//                val workflowAction = PaasRespHelper.orServEx(oozieWFActionService.findByActionId(action.id))
//                if (workflowAction == null) {
//                    oozieWFActionService.createWFAction(oozieAction)
//                } else {
//                    oozieAction.id = workflowAction.id
//                    oozieWFActionService.updateWFAction(oozieAction)
//                }
//            }
            return oozieJob
        } catch (e: Exception) {
            return OozieJob()
        }
    }
}