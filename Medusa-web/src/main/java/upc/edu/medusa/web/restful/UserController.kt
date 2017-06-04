package upc.edu.medusa.web.restful

import com.avaje.ebean.Ebean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.*
import upc.edu.medusa.common.util.Constants
import upc.edu.medusa.datasource.service.MedusaDatasourceReadService
import upc.edu.medusa.datasource.service.MedusaDatasourceReadServiceImpl
import upc.edu.medusa.hadoop.service.HdfsReadServcie
import upc.edu.medusa.oozie.manager.OozieJobManager
import upc.edu.medusa.oozie.service.OozieService
import upc.edu.medusa.query.UserSqlQuery
import upc.edu.medusa.query.service.MedusaColumnInfoWriteService
import upc.edu.medusa.query.service.MedusaTableInfoReadService
import upc.edu.medusa.query.service.MedusaTableInfoWriteService
import upc.edu.medusa.query.utils.SqlQueryConvertUtils
import upc.edu.medusa.user.model.InfoTest
import upc.edu.medusa.user.model.User
import upc.edu.medusa.user.service.MedusaUserReadService
import upc.edu.medusa.user.service.MedusaUserWriteService
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpSession

/**
 * Created by msi on 2017/4/16.
 */


@RestController
@RequestMapping("/api/user")
class UserController @Autowired constructor(
        private val userReadService: MedusaUserReadService,
        private val userWriteService: MedusaUserWriteService,
        private val hdfsReadService: HdfsReadServcie,
        private val oozieservice: OozieService,
        private val userSqlQuery: UserSqlQuery,
        private val medusaDatasourceReadServiceImpl: MedusaDatasourceReadService,
        private val medusaTableInfoReadService: MedusaTableInfoReadService,
        private val medusaColumnInfoWriteService: MedusaColumnInfoWriteService,
        private val medusaTableInfoWriteService: MedusaTableInfoWriteService
) {

    @Autowired
    lateinit var oozieJobManager: OozieJobManager

    @RequestMapping("", method = arrayOf(RequestMethod.GET))
    fun getUser(request: HttpServletRequest): User? {
        val user = userReadService.findUserByName(request.session.getAttribute("name").toString())
        return user
    }

    @RequestMapping("/register", method = arrayOf(RequestMethod.POST))
    fun registerUser(@RequestParam name: String,
                     @RequestParam password: String,
                     @RequestParam email: String,
                     @RequestParam phone: Long,
                     @RequestParam nickName: String): Boolean {
        val user: User = User()
        user.email = email
        user.name = name
        user.nickName = nickName
        user.phone = phone
        user.password = password
        return userWriteService.saveUser(user)
    }

    @RequestMapping("/login", method = arrayOf(RequestMethod.POST))
    fun login(request: HttpServletRequest,
              name: String,
              password: String): Boolean {
        val user = userReadService.findUserByName(name) ?: return false
        if (user.password == password) {
            request.session.setAttribute("name", name)
            request.session.setAttribute("password", password)
            return true
        }
        return false
    }

    @RequestMapping("/logout", method = arrayOf(RequestMethod.POST))
    fun logout(request: HttpServletRequest) {
        request.getSession(false)?.invalidate()
    }

    @RequestMapping("/test", method = arrayOf(RequestMethod.GET))
    fun registerUser(): String {
//        hdfsReadService.getAllFiles("/user/fan/oozie-fan",null,null)
//        return hdfsReadService.isDirectory("/user/fan/oozie-fan").toString()
//        oozieJobManager.start("0000005-170511124936324-oozie-fan-W");
//        val medusaDatasourceReadServiceImpl = medusaDatasourceReadServiceImpl.findDatasources()
//        return  userSqlQuery.executeQuerySql(0L, "", "", SqlQueryConvertUtils.showDatabases()).toString()
//        val medusaTableInfoReadService  = medusaTableInfoReadService.queryTableInfoById(1)
//        if (medusaTableInfoWriteService.saveTableInfo("s_test22", "test"))
//            medusaColumnInfoWriteService.saveColumnInfo("test", "s_test22")

        var x = 0
        while (x < 500000) {
            val info : InfoTest = InfoTest()
            info.name = "xiaoming"
            info.sex = "male"
            info.classs = "comnputer"
            info.test_extra = x.toString()
            x++
            Ebean.save(info)
        }

        return ""
    }

    @RequestMapping("/update", method = arrayOf(RequestMethod.POST))
    fun updateUser(request: HttpServletRequest,
                   @RequestParam oldpassword: String,
                   @RequestParam newpassword: String,
                   @RequestParam surepassword: String,
                   @RequestParam email: String,
                   @RequestParam phone: Long?,
                   @RequestParam nickName: String): Boolean {
        val user: User = userReadService.findUserByName(request.session.getAttribute("name").toString())!!
        if (!nickName.isNullOrBlank()) {
            user.nickName = nickName
        }
        if (phone != null) {
            user.phone = phone
        }
        if (!email.isNullOrBlank()) {
            user.email = email
        }
        if (oldpassword == user.password && !oldpassword.isNullOrBlank()) {
            if (newpassword == surepassword)
                user.password = newpassword
            else
                throw Exception("password.is.not.equal")
        }
        return userWriteService.updataUser(user)
    }


}