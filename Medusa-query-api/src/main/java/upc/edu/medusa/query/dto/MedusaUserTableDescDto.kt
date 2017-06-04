package upc.edu.medusa.query.dto

/**
 * Created by msi on 2017/5/17.
 */
/**
 * 数据表结构描述
 */
data class MedusaUserTableDescDto(
        var tableName: String,  // 数据表名称
        var fields : List<MedusaUserTableFieldDto>   // 字段描述
){

    companion object{
        fun buildFromSqlQueryResult( h : MedusaUserSqlQueryResultDto , tableName: String ) : MedusaUserTableDescDto{
            return MedusaUserTableDescDto(tableName, h.data.map { MedusaUserTableFieldDto(it.get("col_name").toString(),
                    it.get("data_type").toString(), it.get("comment").toString()) })
        }
    }
}

/**
 * 数据字段信息内容
 */
data class MedusaUserTableFieldDto(
        var columnName: String, // 数据Column 名称
        var dataType : String,  // 对应的数据类型
        var comment : String    // 注解信息
)