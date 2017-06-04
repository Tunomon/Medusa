package upc.edu.medusa.datasource.util

import upc.edu.medusa.datasource.model.MedusaDatasource
import java.sql.Connection
import java.sql.DriverManager

/**
 * Created by msi on 2017/5/5.
 */
object DBUtil {

    fun testConnection(datasource: MedusaDatasource): Connection {
        Class.forName(datasource.driver)
        DriverManager.setLoginTimeout(10)
        return DriverManager.getConnection(datasource.url, datasource.dbUsername, datasource.dbPassword)
    }
}