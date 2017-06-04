package upc.edu.medusa.oozie.service

import upc.edu.medusa.oozie.model.OozieJob
import upc.edu.medusa.oozie.model.OozieJobTemplate

/**
 * Created by msi on 2017/5/11.
 */
interface OozieJobService {

    /**
     * 同步workflow数据
     * @param oozieJobId    oozie server 的 job id
     * @param template      oozie template 的对象信息
     */
    fun synchronizeWorkflow(oozieJobId: String, template: OozieJobTemplate, nodeName: String?, creatorId: Long?, creatorName: String?, updatorId: Long?, updatorName: String?, orgId: Long?): OozieJob

}