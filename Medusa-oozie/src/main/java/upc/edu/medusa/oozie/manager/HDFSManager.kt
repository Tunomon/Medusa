package upc.edu.medusa.oozie.manager

import com.google.common.base.Throwables
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.springframework.stereotype.Component
import java.io.InputStream

/**
 * Created by msi on 2017/5/11.
 */
@Component
open class HDFSManager {

    companion object {
        var fileSystem: FileSystem? = null
    }

    /**
     * 判断是否是目录
     */
    fun isDirectory(path: String): Boolean {
        return fileSystem!!.isDirectory(Path(path))
    }

    /**
     * 判断路径是否存在
     */
    fun exist(path: String): Boolean {
        return fileSystem!!.exists(Path(path))
    }

    /**
     * 创建文件夹
     */
    fun createDir(path: String): Boolean {
        if (exist(path)) return false
        return fileSystem!!.mkdirs(Path(path))
    }

    /**
     * 删除某个路径
     * @param recursive: 是否级联删除。如果目标是个目标, 且为true, 那么会级联删除。否则抛出异常。
     *                   如果目标是文件, true和false随意
     */
    fun delete(path: String, recursive: Boolean): Boolean {
        if (!exist(path)) return true
        return fileSystem!!.delete(Path(path), recursive)
    }

    /**
     * 创建文件
     * @param override: 是否覆盖, 如果为true表示覆盖源文件。如果false, 在文件存在的时候抛出异常
     */
    fun createFile(path: String, inputStream: InputStream, override: Boolean): Boolean {
        val outputStream = fileSystem!!.create(Path(path), override)
        IOUtils.copyBytes(inputStream, outputStream, 4096, true)
        return true
    }

    /**
     * 下载文件, 返回输入流
     */
    fun downloadFile(path: String): FSDataInputStream {
        return fileSystem!!.open(Path(path))
    }

    /**
     * 获取所有的文件路径
     */
    fun listChildPaths(path: String): MutableList<String> {
        val paths: MutableList<String> = mutableListOf()
        val files = fileSystem!!.listFiles(Path(path), true)
        while (files.hasNext()) {
            val next = files.next()
            paths.add(next.path.toString())
        }
        return paths
    }

    /**
     * 校验HDFS是否可达
     */
    fun isHealth(): Boolean {
        try{
            return exist("/")
        } catch (e: Exception) {
            System.out.println("HDFS is not available, cause by ${Throwables.getStackTraceAsString(e)}")
            return false
        }
    }
}