package upc.edu.medusa.oozie.xml.node

import java.io.Serializable

/**
 * Created by msi on 2017/5/11.
 */
class Global() : Serializable {
    // 其他一些属性值, 如namenode等, 暂不使用
    // ......

    // configuration中的属性值
    var configuration: MutableMap<String, String> = mutableMapOf()

    constructor(configuration: MutableMap<String, String>) : this() {
        this.configuration = configuration
    }

    // to xml
    fun toXML(): String {
        val xml = StringBuilder("<global>\n")
        if (configuration.isNotEmpty()) {
            xml.append("<configuration>\n")
            for ((key, value) in configuration) {
                xml.append("<property>\n<name>$key</name>\n<value>$value</value>\n</property>\n")
            }
            xml.append("</configuration>\n")
        }
        xml.append("</global>\n")
        return xml.toString()
    }
}