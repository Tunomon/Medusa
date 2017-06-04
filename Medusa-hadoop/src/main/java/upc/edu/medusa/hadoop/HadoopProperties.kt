package upc.edu.medusa.hadoop

import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * Created by msi on 2017/5/8.
 */
@ConfigurationProperties(prefix = "hadoop")
class HadoopProperties {

    /**
     * hadoop name node 地址
     */
    var nameNode: String? = null

}