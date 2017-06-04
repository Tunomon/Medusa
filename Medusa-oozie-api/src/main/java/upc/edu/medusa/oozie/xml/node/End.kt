package upc.edu.medusa.oozie.xml.node

import java.io.Serializable

/**
 * Created by msi on 2017/5/11.
 */
class End() : Serializable {

    var name: String? = null

    constructor(name: String) : this(){
        this.name = name
    }

    fun toXML(): String{
        return "<end name=\"$name\"/>\n"
    }
}
