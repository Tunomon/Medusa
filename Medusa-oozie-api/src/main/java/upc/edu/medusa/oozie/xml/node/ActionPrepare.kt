package upc.edu.medusa.oozie.xml.node

import java.io.Serializable

/**
 * Created by msi on 2017/5/11.
 */
class ActionPrepare() : Serializable {

    var type: PrepareType? = null
    var path: String? = null

    constructor(type: PrepareType, path: String) : this(){
        this.type = type
        this.path = path
    }

    fun toXML(): String {
        return "<${type!!.xmlElement} path=\"$path\"/>\n"
    }

    /**
     * action 中预处理动作类型
     * @param xmlElement 在生成 xml 时该动作的 element 名称
     */
    enum class PrepareType(val xmlElement: String) {
        DELETE("delete"), MKDIR("mkdir")
    }

}