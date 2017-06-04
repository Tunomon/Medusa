package upc.edu.medusa.oozie.utils

import org.apache.commons.lang.StringEscapeUtils
import java.util.*

/**
 * Created by msi on 2017/5/11.
 */
/**
 * 如果对应的key是null或者blank, 则赋值
 */
fun MutableMap<String, String>.putIfAbsentOrBlank(key: String, value: String) {
    if (this[key].isNullOrBlank()) {
        this[key] = value
    }
}

/**
 * 获取两个时间之间的差(秒)
 */
fun Date?.secondsBetween(end: Date?): Long? {
    if (this != null && end != null) {
        return Math.abs((end.time - this.time) / 1000)
    }
    return null
}

/**
 * 如果 String 是 null, 则抛出异常
 */
fun String?.nullThrowException(error: String) {
    if (this == null) {
        throw IllegalArgumentException(error)
    }
}

/**
 * 如果 String 是 null 或者 blank, 则抛出异常
 */
fun String?.blankThrowException(error: String) {
    if (this.isNullOrBlank()) {
        throw IllegalArgumentException(error)
    }
}

/**
 * 如果 MutableList 是 null 或者 empty, 则抛出异常
 */
fun <T> MutableList<T>?.emptyThrowException(error: String) {
    if (this == null || this.isEmpty()) {
        throw IllegalArgumentException(error)
    }
}

/**
 * 将 Long 返回 Date
 */
fun Long?.toDate(): Date? {
    return Date(this ?: return null)
}

/**
 * 将 xml 的特殊字符转义
 */
fun String?.escapeXml(): String? {
    this ?: return null
    return StringEscapeUtils.escapeXml(this)
}