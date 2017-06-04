package upc.edu.medusa.oozie.xml.node

import java.io.Serializable

/**
 * Created by msi on 2017/5/11.
 */
class ActionError() : Serializable {
    var to: String? = null

    constructor(to: String) : this(){
        this.to = to
    }
}