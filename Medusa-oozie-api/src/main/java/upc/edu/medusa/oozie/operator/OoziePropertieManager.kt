package upc.edu.medusa.oozie.operator

import upc.edu.medusa.oozie.dto.DefaultProperty

/**
 * Created by msi on 2017/5/11.
 */
interface OoziePropertieManager {

    /**
     * oozie work path
     */
    fun getOozieWorkPath(): String

    /**
     * oozie sql temp path
     */
    fun getSqlTempPath(): String

    /**
     * 显示默认节点信息
     */
    fun showDefaultProperty(): DefaultProperty

}