package upc.edu.medusa.query

import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

/**
 * Created by msi on 2017/5/5.
 */
@Configuration
@EnableConfigurationProperties(HorusQueryHiveProperties::class)
@ComponentScan("upc.edu.medusa.query")
open class MedusaQueryConfiguration {
}