package upc.edu.medusa.hadoop


import org.apache.hadoop.fs.FileSystem
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import java.net.URI
/**
 * Created by msi on 2017/5/8.
 */
@Configuration
@ComponentScan("upc.edu.medusa.hadoop")
@EnableConfigurationProperties(HadoopProperties::class)
open class HadoopConfiguration {

//    @Bean
//    @ConditionalOnMissingBean(org.apache.hadoop.conf.Configuration::class)
//    open fun configuration(hadoopProperties: HadoopProperties): org.apache.hadoop.conf.Configuration {
//        val configuration = org.apache.hadoop.conf.Configuration()
//        configuration["fs.defaultFS"] = hadoopProperties.nameNode
//        return configuration
//    }

    @Bean
    @ConditionalOnMissingBean(FileSystem::class)
    // @ConditionalOnBean(org.apache.hadoop.conf.Configuration::class)
    open fun fileSystem(hadoopProperties: HadoopProperties): FileSystem {
        val configuration = org.apache.hadoop.conf.Configuration()
        configuration["fs.defaultFS"] = hadoopProperties.nameNode
        // 上传操作目前暂时都是hdfs
        return FileSystem.get(URI(hadoopProperties.nameNode), configuration, "hdfs")
    }
}