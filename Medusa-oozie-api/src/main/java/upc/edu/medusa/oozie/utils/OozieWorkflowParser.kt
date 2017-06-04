package upc.edu.medusa.oozie.utils

import com.google.common.base.Splitter
import org.w3c.dom.Document
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import upc.edu.medusa.common.util.Splitters
import upc.edu.medusa.common.util.XmlHelper
import upc.edu.medusa.oozie.model.OozieJobTemplate
import upc.edu.medusa.oozie.xml.action.Sqoop
import upc.edu.medusa.oozie.xml.app.WorkflowApp
import upc.edu.medusa.oozie.xml.node.*

/**
 * Created by msi on 2017/5/11.
 */
object OozieWorkflowParser {

    // 标签名称
    val NODE_WORKFLOW_APP = "workflow-app"
    val NODE_GLOBAL = "global"
    val NODE_START = "start"
    val NODE_END = "end"
    val NODE_KILL = "kill"
    val NODE_MESSAGE = "message"
    val NODE_DECISION = "decision"
    val NODE_FORK = "fork"
    val NODE_JOIN = "join"
    val NODE_SWITCH = "switch"
    val NODE_CASE = "case"
    val NODE_DEFAULT = "default"
    val NODE_ACTION = "action"
    val NODE_OK = "ok"
    val NODE_ERROR = "error"
    val NODE_PREPARE = "prepare"
    val NODE_DELETE = "delete"
    val NODE_MKDIR = "mkdir"
    val NODE_JOB_XML = "job-xml"
    val NODE_CONFIGURATION = "configuration"
    val NODE_PROPERTY = "property"
    val NODE_NAME = "name"
    val NODE_VALUE = "value"
    val NODE_PATH = "path"

    // mapreduce
    val NODE_MAPREDUCE = "map-reduce"
    val NODE_JAR = "jar"

    // spark
    val NODE_SPARK = "spark"
    val NODE_MASTER = "master"
    val NODE_MODE = "mode"
    val NODE_CLASS = "class"
    val NODE_JARS = "jars"
    val NODE_SPARK_OPTS = "spark-opts"
    val NODE_ARG = "arg"

    // java main
    val NODE_JAVA = "java"
    val NODE_JAVA_OPTS = "java-opts"

    // sqoop
    val NODE_SQOOP = "sqoop"
    val NODE_COMMAND = "command"
    val NODE_JOB_TRACKER = "job-tracker"
    val NODE_NAME_NODE = "name-node"

    // hive
    val NODE_HIVE = "hive"
    val NODE_SCRIPT = "script"
    val NODE_PARAM = "param"

    // hive2
    val NODE_HIVE2 = "hive2"
    val NODE_JDBC_URL = "jdbcUrl"
    val NODE_PASSWORD = "password"

    // email
    val NODE_EMAIL = "email"
    val NODE_TO = "to"
    val NODE_CC = "cc"
    val NODE_SUBJECT = "subject"
    val NODE_BODY = "body"

    // fs
    val NODE_FS = "fs"
    // val NODE_DELETE = "delete"
    // val NODE_MKDIR = "mkdir"
    val NODE_MOVE = "move"
    val NODE_CHMOD = "chmod"
    val NODE_TOUCHZ = "touchz"
    val NODE_CHGRP = "chgrp"
    val NODE_RECURSIVE = "recursive"  // 属性表达方式


    // 属性名称
    val ATTR_TO = "to"
    val ATTR_NAME = "name"
    val ATTR_PATH = "path"
    val ATTR_SOURCE = "source"
    val ATTR_TARGET = "target"
    val ATTR_GROUP = "group"
    val ATTR_PERMISSIONS = "permissions"
    val ATTR_DIR_FILES = "dir-files"
    val ATTR_START = "start"

    /**
     * 将 xml 解析成 Workflow App, 注意: 不做内容的校验, 内容的校验放在下一步操作中
     */
    fun parseWorkflowApp(template: OozieJobTemplate): WorkflowApp {

        val workflowApp = WorkflowApp()
        val document = XmlHelper.toDocument(template.webXml!!.byteInputStream())

        // workflow app 属性
        workflowApp.name = template.jobName
        // workflow global节点
        workflowApp.global = getGlobal(document)
        // workflow 控制节点
        workflowApp.start = getStart(document)
        workflowApp.end = getEnd(document)
        workflowApp.kills.addAll(getKills(document))
        workflowApp.decisions.addAll(getDecisions(document))
        // TODO : fork join
//        workflowApp.forks.addAll(getForks(document))
//        workflowApp.joins.addAll(getJoins(document))

        // workflow 动作节点
        toList(XmlHelper.getNodeList("/$NODE_WORKFLOW_APP/$NODE_ACTION", document)).forEach { action ->
            // 获取Action下的任务节点
            val actionNode = XmlHelper.getChildrenSingleNodeIn(action, arrayListOf(
                    NODE_MAPREDUCE, NODE_SQOOP, NODE_HIVE, NODE_HIVE2, NODE_SPARK, NODE_JAVA, NODE_EMAIL))

            // action name
            val actionName = XmlHelper.getAttrValue(action, ATTR_NAME) ?: ""

            // 获取 ok 和 error
            val ok = XmlHelper.getAttrValue(XmlHelper.getChildrenSingleNode(action, NODE_OK), ATTR_TO) ?: ""
            val error = XmlHelper.getAttrValue(XmlHelper.getChildrenSingleNode(action, NODE_ERROR), ATTR_TO) ?: ""


            when (actionNode.nodeName) {
//                NODE_MAPREDUCE -> {  // map-reduce
//                    workflowApp.actions.add(Action(mapReduce = getMapReduce(actionNode, actionName, ok, error)))
//                }
//                NODE_SPARK -> { // spark
//                    workflowApp.actions.add(Action(spark = getSpark(actionNode, actionName, ok, error)))
//                }
//                NODE_JAVA -> {  // java main
//                    workflowApp.actions.add(Action(java = getJava(actionNode, actionName, ok, error)))
//                }
                NODE_SQOOP -> { // sqoop
                    workflowApp.actions.add(Action(sqoop = getSqoop(actionNode, actionName, ok, error)))
                }
//                NODE_HIVE -> {  // hive
//                    workflowApp.actions.add(Action(hive = getHive(actionNode, actionName, ok, error)))
//                }
//                NODE_HIVE2 -> { // hive2
//                    workflowApp.actions.add(Action(hive2 = getHive2(actionNode, actionName, ok, error)))
//                }
//                NODE_EMAIL -> { // email
//                    workflowApp.actions.add(Action(email = getEmail(actionNode, actionName, ok, error)))
//                }
//                NODE_FS -> {
//                    workflowApp.actions.add(Action(fs = getFs(actionNode, actionName, ok, error)))
//                }
            }
        }

        return workflowApp
    }

    //global
    private fun getGlobal(document: Document): Global? {
        val globals: MutableMap<String, String> = mutableMapOf()
        val globalNode = XmlHelper.getNode("/$NODE_WORKFLOW_APP/$NODE_GLOBAL", document) ?: return null
        val configNode = XmlHelper.getChildrenNode(globalNode, NODE_CONFIGURATION) ?: return Global(globals)

        XmlHelper.getChildrenNodes(configNode, NODE_PROPERTY).forEach {
            val name = XmlHelper.getChildrenSingleNode(it, NODE_NAME)
            val value = XmlHelper.getChildrenSingleNode(it, NODE_VALUE)
            globals.put(name.textContent, value.textContent)
        }
        return Global(globals)
    }

    // start 节点
    private fun getStart(document: Document): Start? {
        val startNode = XmlHelper.getNode("/$NODE_WORKFLOW_APP/$NODE_START", document)
        if (startNode != null) {
            return Start(XmlHelper.getAttrValue(startNode, ATTR_TO) ?: "")
        }
        return null
    }

    // end 节点
    private fun getEnd(document: Document): End? {
        val startNode = XmlHelper.getNode("/$NODE_WORKFLOW_APP/$NODE_END", document)
        if (startNode != null) {
            return End(XmlHelper.getAttrValue(startNode, ATTR_NAME) ?: "")
        }
        return null
    }

    // kill 节点
    private fun getKills(document: Document): MutableList<Kill> {
        val kills: MutableList<Kill> = mutableListOf()
        val killNodes = toList(XmlHelper.getNodeList("/$NODE_WORKFLOW_APP/$NODE_KILL", document))

        killNodes.forEach { killNode ->
            // message 节点
            val message = XmlHelper.getChildrenSingleNode(killNode, NODE_MESSAGE)
            kills.add(Kill(XmlHelper.getAttrValue(killNode, ATTR_NAME) ?: "", message.textContent))
        }
        return kills
    }

    // decision 节点
    private fun getDecisions(document: Document): MutableList<Decision> {
        val decisions: MutableList<Decision> = mutableListOf()
        val decisionNodes = toList(XmlHelper.getNodeList("/$NODE_WORKFLOW_APP/$NODE_DECISION", document))
        decisionNodes.forEach { decisionNode ->
            val deci = Decision()
            deci.name = XmlHelper.getAttrValue(decisionNode, ATTR_NAME)
            // switch 节点
            val switchNode = XmlHelper.getChildrenSingleNode(decisionNode, NODE_SWITCH)

            // 1. 获取case节点
            XmlHelper.getChildrenNodes(switchNode, NODE_CASE).forEach { case ->
                deci.cases.add(Decision.Case(XmlHelper.getAttrValue(case, ATTR_TO) ?: "", case.textContent))
            }

            // 2. 获取default节点
            val default = XmlHelper.getChildrenSingleNode(switchNode, NODE_DEFAULT)
            deci.defaultTo = XmlHelper.getAttrValue(default, ATTR_TO)

            decisions.add(deci)
        }
        return decisions
    }


    // sqoop
    private fun getSqoop(sqoop: Node, name: String, ok: String, error: String): Sqoop {
        val sq = Sqoop()
        // 基本属性
        sq.name = name
        sq.ok = ActionOk(ok)
        sq.error = ActionError(error)

        // prepare
        sq.prepare.addAll(getPrepare(sqoop))

        // job-xml
        val jobXml = XmlHelper.getChildrenNode(sqoop, NODE_JOB_XML)
        if (jobXml != null) {
            sq.jobXml = jobXml.textContent
        }

        // jobTracker
        val jobTracker = XmlHelper.getChildrenNode(sqoop, NODE_JOB_TRACKER)
        if (jobTracker != null){
            sq.jobTracker = jobTracker.textContent
        }

        // namenode
        val namenode = XmlHelper.getChildrenNode(sqoop, NODE_NAME_NODE)
        if (namenode != null){
            sq.nameNode = namenode.textContent
        }

        // configuration
        sq.configuration.putAll(getConfiguration(sqoop))

        // command
        val command = XmlHelper.getChildrenNode(sqoop, NODE_COMMAND)
        if (command != null) {
            sq.command = command.textContent
        }

        // arg
        sq.args.addAll(getArg(sqoop))

        return sq
    }


    // prepare
    private fun getPrepare(node: Node): MutableList<ActionPrepare> {
        val prepares: MutableList<ActionPrepare> = arrayListOf()

        // prepare
        val prepareNode = XmlHelper.getChildrenNode(node, NODE_PREPARE)

        // delete 节点
        XmlHelper.getChildrenNodes(prepareNode, NODE_DELETE).forEach {
            prepares.add(ActionPrepare(ActionPrepare.PrepareType.DELETE, XmlHelper.getAttrValue(it, ATTR_PATH) ?: ""))
        }

        // mkdir 节点
        XmlHelper.getChildrenNodes(prepareNode, NODE_MKDIR).forEach {
            prepares.add(ActionPrepare(ActionPrepare.PrepareType.MKDIR, XmlHelper.getAttrValue(it, ATTR_PATH) ?: ""))
        }
        return prepares
    }

    // configuration
    private fun getConfiguration(node: Node): MutableMap<String, String> {
        val configurations: MutableMap<String, String> = mutableMapOf()

        // configuration node
        val configurationNode = XmlHelper.getChildrenNode(node, NODE_CONFIGURATION)

        XmlHelper.getChildrenNodes(configurationNode, NODE_PROPERTY).forEach {
            val name = XmlHelper.getChildrenSingleNode(it, NODE_NAME)
            val value = XmlHelper.getChildrenSingleNode(it, NODE_VALUE)
            configurations[name.textContent] = value.textContent
        }

        return configurations
    }

    // param
    private fun getParam(node: Node): MutableMap<String, String> {
        val params: MutableMap<String, String> = mutableMapOf()
        XmlHelper.getChildrenNodes(node, NODE_PARAM).forEach {
            val pair = Splitters.EQUALS.splitToList(it.textContent)
            if (pair.size != 2) {
                throw Exception("workflow.app.hive.param.error")
            }
            params[pair[0]] = pair[1]
        }
        return params
    }

    // arg
    private fun getArg(node: Node): MutableList<String> {
        val args: MutableList<String> = mutableListOf()
        XmlHelper.getChildrenNodes(node, NODE_ARG).forEach {
            args.add(it.textContent)
        }
        return args
    }


    // 将 NodeList 转化为 list
    private fun toList(nodeList: NodeList?): MutableList<Node> {
        val list: MutableList<Node> = mutableListOf()
        if (nodeList != null && nodeList.length > 0) {
            for (i in 0..nodeList.length - 1) {
                list.add(nodeList.item(i))
            }
        }
        return list
    }
}