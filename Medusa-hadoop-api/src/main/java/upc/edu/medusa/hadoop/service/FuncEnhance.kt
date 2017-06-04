package upc.edu.medusa.hadoop.service

import java.util.*

/**
 * Created by msi on 2017/5/8.
 */
/**
 * 将 Long 返回 Date
 */
fun Long?.toDate(): Date? {
    return Date(this ?: return null)
}

/**
 * 判断字符串是否包含
 */
fun String?.endWithIn(list: List<String>?): Boolean {
    if (this.isNullOrBlank()) {
        return true
    }
    if (list == null || list.isEmpty()) {
        return true
    }
    val suffix = this!!.substring(this.lastIndexOf(".") + 1)
    if (suffix.equals(this)) {
        return false
    }
    return list.contains(suffix)
}
