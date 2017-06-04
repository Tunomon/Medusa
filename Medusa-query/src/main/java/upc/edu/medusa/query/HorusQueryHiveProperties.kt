package upc.edu.medusa.query

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Created by msi on 2017/5/16.
 */
@ConfigurationProperties(prefix = "hive")
class HorusQueryHiveProperties {

    var url: String? = null

    var username: String = ""

    var password: String = ""

}