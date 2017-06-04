package upc.edu.medusa.user.service

import com.avaje.ebean.Ebean
import org.springframework.stereotype.Service
import upc.edu.medusa.user.model.User
import upc.edu.medusa.user.service.MedusaUserReadService

/**
 * Created by msi on 2017/4/14.
 */
@Service
class MedusaUserReadServiceImpl : MedusaUserReadService{

    override fun findUserByName(name: String): User? {
        val user =  User.find.where().eq("name",name).findUnique()
        return user
    }
}