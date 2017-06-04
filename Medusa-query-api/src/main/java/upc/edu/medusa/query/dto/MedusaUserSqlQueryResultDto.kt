package upc.edu.medusa.query.dto

/**
 * Created by msi on 2017/5/16.
 */
data class MedusaUserSqlQueryResultDto(
        var columns : List<String>,   //结果Column 条目数量
        var data : List<Map<String, Any>>,  // 返回对应的结果信息
        var columnSize : Int,    //列的数量信息
        var dataSize : Int,  //数据的数量信息
        var log : List<String>// 对应sql 执行日志信息内容
)
