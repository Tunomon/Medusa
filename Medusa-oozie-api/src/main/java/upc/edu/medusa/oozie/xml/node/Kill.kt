package upc.edu.medusa.oozie.xml.node

import java.io.Serializable

/**
 * Created by msi on 2017/5/11.
 */
class Kill () : Serializable {
    var name: String? = null
    var message: String? = null

    constructor(name: String, message: String?) : this(){
        this.name = name
        this.message = message
    }

    fun toXML(): String{
        val prefix = "<kill name=\"$name\">\n"
        var messageXML = ""
        if(message != null){
            messageXML = "<message>$message</message>\n"
        }
        val suffix = "</kill>\n"
        return prefix + messageXML + suffix
    }
}
