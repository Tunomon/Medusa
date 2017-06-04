package upc.edu.medusa.common.util

import com.google.common.base.Splitter
import com.google.common.collect.Lists

/**
 * Created by msi on 2017/5/9.
 */
object Splitters {

    val DOT = Splitter.on(".").omitEmptyStrings().trimResults()
    val COMMA = Splitter.on(",").omitEmptyStrings().trimResults()
    val COLON = Splitter.on(":").omitEmptyStrings().trimResults()
    val AT = Splitter.on("@").omitEmptyStrings().trimResults()
    val SLASH = Splitter.on("/").omitEmptyStrings().trimResults()
    val SPACE = Splitter.on(" ").omitEmptyStrings().trimResults()
    val UNDERSCORE = Splitter.on("_").omitEmptyStrings().trimResults()
    val EQUALS = Splitter.on("=").omitEmptyStrings().trimResults()
    val SEMICOLON = Splitter.on(";").omitEmptyStrings().trimResults()
    val TAB = Splitter.on("\t").omitEmptyStrings().trimResults()
    val HASH = Splitter.on("#").omitEmptyStrings().trimResults()


    fun splitToLong(sequence: CharSequence, splitter: Splitter): List<Long> {
        val ss = splitter.splitToList(sequence)
        val res = Lists.newArrayListWithCapacity<Long>(ss.size)
        for (s in ss) {
            res.add(java.lang.Long.parseLong(s))
        }
        return res
    }

    fun splitToInteger(sequence: CharSequence, splitter: Splitter): List<Int> {
        val ss = splitter.splitToList(sequence)
        val res = Lists.newArrayListWithCapacity<Int>(ss.size)
        for (s in ss) {
            res.add(Integer.parseInt(s))
        }
        return res
    }
}