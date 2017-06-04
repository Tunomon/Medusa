package upc.edu.medusa.web.restful

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import upc.edu.medusa.common.JsonMapper
import upc.edu.medusa.datasource.service.MedusaDatasourceReadService
import upc.edu.medusa.hadoop.service.HdfsReadServcie
import upc.edu.medusa.oozie.model.OozieJobTemplate
import upc.edu.medusa.oozie.service.OozieService
import upc.edu.medusa.oozie.utils.OozieWorkflowParser
import upc.edu.medusa.oozie.xml.action.Sqoop
import upc.edu.medusa.oozie.xml.app.WorkflowApp
import upc.edu.medusa.query.service.MedusaColumnInfoReadService
import upc.edu.medusa.query.service.MedusaColumnInfoWriteService
import upc.edu.medusa.query.service.MedusaTableInfoReadService
import upc.edu.medusa.query.service.MedusaTableInfoWriteService
import upc.edu.medusa.user.model.User
import upc.edu.medusa.user.service.MedusaUserReadService
import upc.edu.medusa.user.service.MedusaUserWriteService
import upc.edu.medusa.web.restful.service.SqoopArgsService
import java.util.*

/**
 * Created by msi on 2017/5/11.
 */
@RestController
@RequestMapping("/api/oozie")
class OozieJobs @Autowired constructor(
        private val hdfsReadService: HdfsReadServcie,
        private val oozieservice: OozieService,
        private val datasourceService: MedusaDatasourceReadService,
        private val sqoopArgsService: SqoopArgsService,
        private val medusaColumnInfoWriteService: MedusaColumnInfoWriteService,
        private val medusaTableInfoWriteService: MedusaTableInfoWriteService
) {

    @RequestMapping("/import/submit", method = arrayOf(RequestMethod.POST))
    fun submitImportJob(@RequestParam jobName: String,
                        @RequestParam importDatabase: String,
                        @RequestParam importTable: String,
                        @RequestParam hiveTable: String,
                        @RequestParam hiveDatabase: String,
                        @RequestParam(required = false) partitionKey: String,
                        @RequestParam(required = false) partitionValue: String,
                        @RequestParam overwrite: String): Boolean {
        val template = OozieJobTemplate()
        template.jobName = jobName
        val datasource = datasourceService.findDatasourceByname(importDatabase)
        template.webXml = """<?xml version="1.0" encoding="UTF-8"?>
        <workflow-app name="workflow-app-sqoop" xmlns="uri:oozie:workflow:0.2">
        <start to="sqoop"/>
        <action name="sqoop">
                <sqoop xmlns="uri:oozie:sqoop-action:0.2">
                        <job-tracker>localhost:8032</job-tracker>
                        <name-node>hdfs://192.168.1.130:9000</name-node>
                        <job-xml>hdfs://192.168.1.130:9000/user/fan/conf/hive-site.xml</job-xml>
                        <configuration>
                                <property>
                                        <name>hive.metastore.uris</name>
                                        <value>thrift://localhost:9083</value>
                                </property>
                                <property>
                                        <name>mapred.job.queue.name</name>
                                        <value>default</value>
                                </property>
                                </configuration>
                        <command>""" + sqoopArgsService.getImportSqoopObject(datasource, importTable, hiveTable, hiveDatabase, partitionKey, partitionValue, overwrite) +
                """</command>
                 </sqoop>
                 <ok to="end"/>
                <error to="kill"/>
        </action>
        <end name="end"/>
        <kill name="kill">
       <message>Sqoop failed, error message[\$\{wf:errorMessage(wf:lastErrorNode())}]</message>
        </kill>
        </workflow-app>"""
        val workflowApp = OozieWorkflowParser.parseWorkflowApp(template)
        oozieservice.submitWorkflow(workflowApp, Properties())

//        if (medusaTableInfoWriteService.saveTableInfo(hiveTable, hiveDatabase))
//            medusaColumnInfoWriteService.saveColumnInfo(hiveDatabase, hiveTable)

        return true
    }

    @RequestMapping("/export/submit", method = arrayOf(RequestMethod.POST))
    fun submitExportJob(@RequestParam jobName: String,
                        @RequestParam exportHiveDatabase: String,
                        @RequestParam exportHiveTable: String,
                        @RequestParam exportDatabase: String,
                        @RequestParam exportTable: String): Boolean {
        val template = OozieJobTemplate()
        template.jobName = jobName
        val datasource = datasourceService.findDatasourceByname(exportDatabase)
        template.webXml = """<?xml version="1.0" encoding="UTF-8"?>
        <workflow-app name="workflow-app-sqoop" xmlns="uri:oozie:workflow:0.2">
        <start to="sqoop"/>
        <action name="sqoop">
                <sqoop xmlns="uri:oozie:sqoop-action:0.2">
                        <job-tracker>localhost:8032</job-tracker>
                        <name-node>hdfs://192.168.1.130:9000</name-node>
                        <job-xml>hdfs://192.168.1.130:9000/user/fan/conf/hive-site.xml</job-xml>
                        <configuration>
                                <property>
                                        <name>hive.metastore.uris</name>
                                        <value>thrift://localhost:9083</value>
                                </property>
                                <property>
                                        <name>mapred.job.queue.name</name>
                                        <value>default</value>
                                </property>
                        </configuration>
                        <command>""" + sqoopArgsService.getExportSqoopObject(datasource, exportHiveDatabase, exportHiveTable, exportDatabase, exportTable) +
                """</command>
                 </sqoop>
                 <ok to="end"/>
                <error to="kill"/>
        </action>
        <end name="end"/>
        <kill name="kill">
       <message>Sqoop failed, error message[\$\{wf:errorMessage(wf:lastErrorNode())}]</message>
        </kill>
        </workflow-app>"""
        val workflowApp = OozieWorkflowParser.parseWorkflowApp(template)
        oozieservice.submitWorkflow(workflowApp, Properties())

        return true
    }


}