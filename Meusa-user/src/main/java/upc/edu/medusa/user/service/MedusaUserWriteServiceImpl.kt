package upc.edu.medusa.user.service

import com.avaje.ebean.Ebean
import org.springframework.stereotype.Service
import upc.edu.medusa.user.model.User
import upc.edu.medusa.user.service.MedusaUserWriteService

/**
 * Created by msi on 2017/4/18.
 */
@Service
class MedusaUserWriteServiceImpl : MedusaUserWriteService{

    override fun saveUser(user: User): Boolean {
        Ebean.save(user)
        return true
    }

    override fun updataUser(user: User): Boolean {
        Ebean.update(user)
        return  true
    }
}