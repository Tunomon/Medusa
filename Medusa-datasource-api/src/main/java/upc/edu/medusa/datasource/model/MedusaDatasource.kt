package upc.edu.medusa.datasource.model

import com.avaje.ebean.Model
import java.io.Serializable
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table

/**
 * Created by msi on 2017/4/19.
 */

@Entity
@Table(name = "medusa_datasources")
class MedusaDatasource : Serializable {
    companion object {
        val find = object : Model.Find<Long, MedusaDatasource>() {}
    }
    @Id
    var id: Long? = null
    var name: String? = null            //数据源名称
    var type: Int? = null               //数据源类型 枚举
    var host: String? = null            //数据源主机host
    var port: Int? = null               //数据源主机port
    var driver: String? = null          //驱动
    var url: String? = null             //链接
    var dbUsername: String? = null      //数据库用户名
    var dbPassword: String? = null      //密码
    @Transient
    var typeString: String? = null      //传给前台的数据类型

    /**
     * 数据源类型枚举
     */
    enum class Type(val value: Int, val desc: String, val port: Int, val driver: String, val connect: String) {
        MySQL(1, "MySQL", 3306, "com.mysql.jdbc.Driver", "jdbc:mysql://"),
        SQLServer(2, "SQLServer", 1433, "net.sourceforge.jtds.jdbc.Driver", "jdbc:jtds:sqlserver://"),
        Hive(3, "Hive", 10000, "org.apache.hive.jdbc.HiveDriver", "jdbc:hive2://"),
        Oracle(4, "Oracle", 1521, "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@");

        companion object {
            fun from(value: Int?): Type? = Type.values().filter { it.value == value }.firstOrNull()
        }

    }
}