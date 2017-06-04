package upc.edu.medusa.oozie.xml.node

import upc.edu.medusa.oozie.xml.action.BaseAction
import upc.edu.medusa.oozie.xml.action.Sqoop
import java.io.Serializable

/**
 * Created by msi on 2017/5/9.
 */
data class Action(
        var sqoop: Sqoop? = null
) : Serializable {

    fun toXML(): String {
        for (field in this.javaClass.declaredFields) {
            if (field.get(this) != null) {
                val ba: BaseAction = field.get(this) as BaseAction
                return ba.toXML()
            }
        }
        return ""
    }
}