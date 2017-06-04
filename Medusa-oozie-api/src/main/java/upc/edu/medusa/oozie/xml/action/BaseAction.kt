package upc.edu.medusa.oozie.xml.action

import upc.edu.medusa.oozie.xml.node.ActionError
import upc.edu.medusa.oozie.xml.node.ActionOk
import upc.edu.medusa.oozie.xml.node.ActionPrepare

/**
 * Created by msi on 2017/5/9.
 */
abstract class BaseAction {
    abstract var name: String?
    abstract var ok: ActionOk?
    abstract var error: ActionError?

    /**
     * action 在 xml 中的 element 名称, 子类务必初始化
     */
    abstract val xmlLabel: String

    /**
     * 预处理动作
     * 分 delete 和 mkdir 两种, 注意添加时 delete 必须在 mkdir 之前
     */
    val prepare: MutableList<ActionPrepare> = mutableListOf()

    val configuration: MutableMap<String, String> = mutableMapOf()

    abstract fun toXML(): String

    /**
     * prepare 中的动作
     */
    fun prepareXML(): String {
        val xml = StringBuilder()
        if(prepare.isNotEmpty()){
            xml.append("<prepare>\n")
            for(pre in prepare){
                xml.append(pre.toXML())
            }
            xml.append("</prepare>\n")
        }
        return xml.toString()
    }

    /**
     * configuration 中的配置
     */
    fun configurationXML(): String {
        val xml = StringBuilder()
        if(configuration.isNotEmpty()){
            xml.append("<configuration>\n")
            for((key, value) in configuration){
                xml.append("<property>\n<name>$key</name>\n<value>$value</value>\n</property>\n")
            }
            xml.append("</configuration>\n")
        }
        return xml.toString()
    }

    /**
     * ok 和 error 节点
     */
    fun okAndErrorXML(): String {
        return "<ok to=\"${ok!!.to}\"/>\n<error to=\"${error!!.to}\"/>\n"
    }
}