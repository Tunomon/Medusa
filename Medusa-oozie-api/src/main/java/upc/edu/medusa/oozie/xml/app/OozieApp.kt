package upc.edu.medusa.oozie.xml.app

/**
 * Created by msi .
 * 此类相当于 oozie 中的  workflow.xml ,coordinator.xml 和 bundle.xml 的父类
 */

abstract class OozieApp () {

    abstract var name: String?

    /**
     * oozie xml 描述, 由子类初始化
     */
    abstract val xmlns: String

    /**
     * workflow/coordinator/bundle 三种job 在xml 中的element名称, 由子类初始化
     */
    abstract val xmlLabel: String

    /**
     * 重要!!.. 将一个oozie job 转换为 xml 格式的字符串
     */
    abstract fun toXML(): String

}