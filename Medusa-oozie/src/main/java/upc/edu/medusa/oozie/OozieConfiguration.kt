package upc.edu.medusa.oozie

import org.apache.hadoop.conf.Configuration
import org.apache.oozie.client.XOozieClient
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
/**
 * Created by msi on 2017/5/9.
 */
@org.springframework.context.annotation.Configuration
@ComponentScan("upc.edu.medusa.oozie")
open class OozieConfiguration {

    @org.springframework.context.annotation.Configuration
    @EnableConfigurationProperties(OozieProperties::class)
    open class OozieConfiguration {

        @Bean
        @ConditionalOnMissingBean(Configuration::class)
        open fun configuration(@Value("\${oozie.hdfs}") hdfs: String): Configuration {
            val configuration = Configuration()
            configuration["fs.defaultFS"] = hdfs
            configuration["dfs.replication"] = "1"  // 减少备份数, 默认 3
            return configuration
        }

        @Bean
        @ConditionalOnMissingBean(XOozieClient::class)
        open fun xOozieClient(@Value("\${oozie.site}") site: String): XOozieClient {
            return XOozieClient(site)
        }
    }

}