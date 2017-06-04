package upc.edu.medusa.oozie.xml.node

import java.io.Serializable

/**
 * Created by msi on 2017/5/11.
 */
class Decision () : Serializable {
    var name: String? = null
    var defaultTo: String? = null

    constructor(name: String, defaultTo: String) : this(){
        this.name = name
        this.defaultTo = defaultTo
    }

    val cases: MutableList<Case> = mutableListOf()

    fun toXML(): String {
        val xml: StringBuilder = StringBuilder("<decision name=\"$name\">\n<switch>\n")
        for(case in cases){
            xml.append("<case to=\"${case.to}\">${case.expression}</case>\n")
        }
        xml.append("<default to=\"$defaultTo\"/>\n")
        xml.append("</switch>\n</decision>\n")
        return xml.toString()
    }

    class Case () {

        var to: String? = null
        var expression: String? = null

        constructor(to: String, expression: String) : this(){
            this.to = to
            this.expression = expression
        }
    }
}