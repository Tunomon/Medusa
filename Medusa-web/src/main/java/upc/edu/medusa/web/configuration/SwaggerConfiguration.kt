package upc.edu.medusa.web.configuration

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import springfox.documentation.annotations.ApiIgnore
import springfox.documentation.builders.ApiInfoBuilder
import springfox.documentation.builders.PathSelectors
import springfox.documentation.builders.RequestHandlerSelectors
import springfox.documentation.spi.DocumentationType
import springfox.documentation.spring.web.json.Json
import springfox.documentation.spring.web.plugins.Docket
import springfox.documentation.swagger2.annotations.EnableSwagger2
import springfox.documentation.swagger2.web.Swagger2Controller
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
/**
 * Created by msi on 2017/4/19.
 */

@RestController
@Configuration
@EnableSwagger2
open class SwaggerConfiguration {

    companion object {
        private val GROUP = "upc"
        private val TITLE = "Medusa Swagger"
        private val DESC = "Swagger API for Medusa"
        private val VERSION = "1.0"

    }

    @Bean
    open fun horusApi(): Docket {
        return Docket(DocumentationType.SWAGGER_2)
                .groupName(GROUP) // 可以根据paths指定不同的分组
                .select()
                .apis(RequestHandlerSelectors.any())
                .paths(PathSelectors.regex("/api/.+"))
                .build()
                .pathMapping("/")
                .apiInfo(ApiInfoBuilder()
                        .title(TITLE)
                        .description(DESC)
                        .version(VERSION)
                        .build())
                .useDefaultResponseMessages(false)
    }

    @Autowired
    private lateinit var swagger2Controller: Swagger2Controller

    @ApiIgnore
    @RequestMapping(value = "/api/swagger", method = arrayOf(RequestMethod.GET))
    fun apiDocument(
            @RequestParam(required = false) group: String?,
            request: HttpServletRequest,
            response: HttpServletResponse): ResponseEntity<Json> {
        response.setHeader("Access-Control-Allow-Origin", "*")
        return swagger2Controller.getDocumentation(group ?: GROUP, request)
    }
}