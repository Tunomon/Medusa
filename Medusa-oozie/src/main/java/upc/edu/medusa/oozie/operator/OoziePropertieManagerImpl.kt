package upc.edu.medusa.oozie.operator

import com.google.common.base.Throwables
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import upc.edu.medusa.oozie.OozieProperties
import upc.edu.medusa.oozie.dto.DefaultProperty
import upc.edu.medusa.oozie.manager.OozieJobManager

/**
 * Created by msi on 2017/5/11.
 */
@Component
class OoziePropertieManagerImpl : OoziePropertieManager {


    @Autowired
    lateinit var oozieJobManager: OozieJobManager
    @Autowired
    lateinit var oozieProperties: OozieProperties

    /**
     * oozie work path
     */
    override fun getOozieWorkPath(): String {
        if (oozieProperties.userName.isNullOrBlank()) {
            oozieJobManager.initOozieJobManager()
        }
        return "/user/${oozieProperties.userName}/oozie-work"
    }

    /**
     * oozie sql temp path
     */
    override fun getSqlTempPath(): String = "${getOozieWorkPath()}/.sqltemp"

    /**
     * 显示默认节点信息
     */
    override fun showDefaultProperty(): DefaultProperty {
        try {
            val property = DefaultProperty()
            property.hiveJdbc = oozieProperties.hiveJdbc
            property.spark = oozieProperties.spark
            property.jobXml = oozieProperties.jobXml
            property.user = oozieProperties.user
            property.userName = oozieProperties.userName
            return property
        } catch(e: Exception) {
            System.out.println("show default property fail, cause by: ${Throwables.getStackTraceAsString(e)}")
            return DefaultProperty()
        }
    }
}