package upc.edu.medusa.common.util

import java.lang.IllegalArgumentException
import java.util.*

/**
 * Created by msi on 2017/4/19.
 */
object RandomStringUtil{
    private val ch = charArrayOf('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1')

    private val random = Random()

    @Synchronized fun createRandomString(length: Int): String {
        if (length > 0) {
            var index = 0
            val temp = CharArray(length)
            var num = random.nextInt()
            for (i in 0..length % 5 - 1) {
                temp[index++] = ch[num and 63]//取后面六位，记得对应的二进制是以补码形式存在的。
                num = num shr 6//63的二进制为:111111
                // 为什么要右移6位？因为数组里面一共有64个有效字符。为什么要除5取余？因为一个int型要用4个字节表示，也就是32位。
            }
            for (i in 0..length / 5 - 1) {
                num = random.nextInt()
                for (j in 0..4) {
                    temp[index++] = ch[num and 63]
                    num = num shr 6
                }
            }
            return String(temp, 0, length)
        } else if (length == 0) {
            return ""
        } else {
            throw IllegalArgumentException()
        }
    }

    //根据指定个数，测试随机字符串函数的重复率
    fun rateOfRepeat(number: Int): Double {
        var repeat = 0
        val str = arrayOfNulls<String>(number)
        for (i in 0..number - 1) {//生成指定个数的字符串
            str[i] = RandomStringUtil.createRandomString(10)
        }
        for (i in 0..number - 1) {//查找是否有相同的字符串
            for (j in i + 1..number - 1 - 1) {
                if (str[i] == str[j])
                    repeat++
            }
        }
        return repeat.toDouble() / number
    }

    @JvmStatic fun main(args: Array<String>) {
        System.out.println(RandomStringUtil.createRandomString(10))
        val rate = RandomStringUtil.rateOfRepeat(10000)//测试10000次的重复率
        println("重复率:" + rate)
    }
}