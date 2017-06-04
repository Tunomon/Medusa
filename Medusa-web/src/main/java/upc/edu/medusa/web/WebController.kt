package upc.edu.medusa.web

import org.apache.catalina.User
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import upc.edu.medusa.user.service.MedusaUserReadServiceImpl
import upc.edu.medusa.user.service.MedusaUserReadService

/**
 * Created by msi on 2017/4/13.
 */

@RestController
@RequestMapping("/api/web")
class WebController @Autowired constructor(
        private val userReadService: MedusaUserReadService
){


}