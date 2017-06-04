package upc.edu.medusa.hadoop.service

import upc.edu.medusa.hadoop.model.HdfsFile

/**
 * Created by msi on 2017/5/8.
 */
interface HdfsReadServcie {
    /**
     * 查询全部文件
     */
    fun getAllFiles(path: String?, type: Int?, suffixs: String?): List<HdfsFile>

    /**
     * 是否正常服务
     */
    fun isHealth(): Boolean

    /**
     * 判断路径是否存在
     */
    fun exist(path: String): Boolean

    /**
     * 判断是否是目录
     */
    fun isDirectory(path: String): Boolean

    /**
     * 根据表路径得到该表大小
     */
    fun getSizeOfTable(path: String?): Long
}