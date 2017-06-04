package upc.edu.medusa.oozie.xml.app

import upc.edu.medusa.oozie.xml.node.*
import java.io.Serializable

/**
 * Created by msi on 2017/5/9.
 */

class WorkflowApp() : OozieApp(), Serializable {

    override val xmlns: String = "uri:oozie:workflow:0.5"
    override val xmlLabel: String = "workflow-app"
    override var name: String? = null
    var global: Global? = null
    var start: Start? = null
    var end: End? = null

    // 常规熟悉
    val kills: MutableList<Kill> = mutableListOf()  // kill 控制节点
    val actions: MutableList<Action> = mutableListOf()  // 动作节点
    val decisions: MutableList<Decision> = mutableListOf()  // decision 控制节点
//    val forks: MutableList<Fork> = mutableListOf()  //  fork控制节点
//    val joins: MutableList<Join> = mutableListOf()  //  join控制节点
//
//    // 其他属性
//    var suffixFs: Fs? = null    // 后置文件操作节点

    constructor(name: String, start: Start, end: End) : this() {
        this.name = name
        this.start = start
        this.end = end
    }

    /**
     * 注入后置 Fs 节点
     */
//    fun injectSuffixFs(fs: Fs?) {
//        if (fs == null) {
//            return
//        }
//        // fs 指向当前 end
//        fs.ok = ActionOk(end!!.name!!)
//        fs.error = ActionError(end!!.name!!)
//        // 迭代actions
//        actions.forEach {
//            for (f in it.javaClass.declaredFields) {
//                f.isAccessible = true
//                if (f.get(it) != null) {
//                    val ba = f.get(it) as BaseAction
//                    if (ba.ok!!.to!!.equals(end!!.name)) {
//                        ba.ok!!.to = fs.name
//                    }
//                    if (ba.error!!.to!!.equals(end!!.name)) {
//                        ba.error!!.to = fs.name
//                    }
//                }
//                f.isAccessible = false
//            }
//        }
//        // 添加到 actions 中
//        actions.add(Action(fs = fs))
//        this.suffixFs = fs
//    }

    override fun toXML(): String {
        val xml = StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<$xmlLabel name=\"$name\" xmlns=\"$xmlns\">\n")
        xml.append(global?.toXML() ?: "")
        xml.append(start!!.toXML())

        for (action in actions) {
            xml.append(action.toXML())
        }

        for (decision in decisions) {
            xml.append(decision.toXML())
        }
//
//        for (fork in forks) {
//            xml.append(fork.toXml())
//        }
//
//        for (join in joins) {
//            xml.append(join.toXml())
//        }

        for (kill in kills) {
            xml.append(kill.toXML())
        }

        xml.append(end!!.toXML())
        xml.append("</$xmlLabel>\n")
        return xml.toString()
    }
}