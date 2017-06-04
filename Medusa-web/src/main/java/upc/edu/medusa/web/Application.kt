package upc.edu.medusa.web

import com.avaje.ebean.EbeanServer
import com.avaje.ebean.EbeanServerFactory
import com.avaje.ebean.config.ServerConfig
import com.avaje.ebean.springsupport.AgentLoaderSupport
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import javax.sql.DataSource
import org.springframework.context.ConfigurableApplicationContext



/**
 * Created by msi on 2017/4/13.
 */


@SpringBootApplication(scanBasePackages = arrayOf("upc.edu"))
open class Application{

    @Bean open fun ebeanServer(@Qualifier("dataSource") dataSource: DataSource): EbeanServer {
        val serverConfig = ServerConfig()
        serverConfig.name = "mysql"
        serverConfig.isDefaultServer = true
        serverConfig.dataSource = dataSource
        // serverConfig.externalTransactionManager = SpringAwareJdbcTransactionManager()
        serverConfig.packages = arrayListOf(
                "upc.edu.medusa.user.model",
                "upc.edu.medusa.datasource.model",
                "upc.edu.medusa.query.model",
                "upc.edu.medusa.oozie.model")
        return EbeanServerFactory.create(serverConfig)
    }
}

fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
//    System.out.print(ctx.getBean("dataSource"))
}