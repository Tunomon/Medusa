package upc.edu.medusa.hadoop.service

import com.google.common.base.Throwables
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import upc.edu.medusa.common.JsonMapper
import upc.edu.medusa.common.constants.JacksonType
import upc.edu.medusa.hadoop.model.HdfsFile

/**
 * Created by msi on 2017/5/9.
 */
@Service
class HdfsReadServiceImpl : HdfsReadServcie {

    @Autowired
    lateinit var fileSystem: FileSystem

    override fun getAllFiles(path: String?, type: Int?, suffixs: String?): List<HdfsFile> {
        // 查出所有的files, 并过滤, (默认根路径)
        var pathT = path
        if (path.isNullOrBlank()) {
            pathT = "/"
        }
        val p = Path(pathT)

        // 获取后缀
        var sList: List<String> = mutableListOf()
        if (!suffixs.isNullOrBlank()) {
            sList = JsonMapper.JSON_NON_DEFAULT_MAPPER.mapper.readValue(suffixs, JacksonType.LIST_OF_STRING)
        }
        val files = fileSystem.listStatus(p)
                .toMutableList()
                .map {
                    HdfsFile.build(it)
                }
                .filter {
                    if (type == null)
                        true
                    else
                        it.type == type
                }
                .filter {
                    if (it.isIfDir()) {
                        true
                    } else {
                        it.name.endWithIn(sList)
                    }
                }
        return files
    }

    override fun isHealth(): Boolean {
        return exist("/")
    }

    override fun exist(path: String): Boolean {
        return fileSystem.exists(Path(path))
    }

    override fun isDirectory(path: String): Boolean {
        return fileSystem.isDirectory(Path(path))
    }

    /**
     * 根据表路径得到该表大小
     */
    override fun getSizeOfTable(path: String?): Long {
        path ?: return 0L
        val allFiles = getAllFiles(path, null, null)
        var size: Long = 0L
        if (allFiles.isEmpty()) {
            size += 0L
        } else
        //如果里面类型是目录,那么说明是分区表,循环加分区表中的文件大小
            size += getSizeIterate(allFiles)
        return size
    }

    private fun getSizeIterate(fileList: List<HdfsFile>): Long {
        var size = 0L
        fileList.forEach {
            if (it.type == 1) {
                size += getSizeOfTable(it.path)
            } else {
                size += it.originalSize!!
            }
        }
        return size
    }
}