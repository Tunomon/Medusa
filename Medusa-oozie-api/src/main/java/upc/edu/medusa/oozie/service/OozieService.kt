package upc.edu.medusa.oozie.service

import upc.edu.medusa.oozie.model.OozieJob
import upc.edu.medusa.oozie.model.OozieJobTemplate
import upc.edu.medusa.oozie.xml.app.WorkflowApp
import java.util.*

/**
 * Created by msi on 2017/5/9.
 */
interface OozieService{

    /**
     *  提交一个 WorkflowApp
     */
    fun submitWorkflow(workflowApp: WorkflowApp, jobProperties: Properties): Boolean

//    /**
//     * 开始一个Job
//     * @param id    oozie job 的 id
//     * 只能是 PREP 状态的job
//     */
//    fun start(id: Long, userId: Long, userName: String): Boolean


}
