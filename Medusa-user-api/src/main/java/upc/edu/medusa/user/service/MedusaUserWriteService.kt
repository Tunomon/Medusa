package upc.edu.medusa.user.service

import upc.edu.medusa.user.model.User

/**
 * Created by msi on 2017/4/18.
 */
interface MedusaUserWriteService {

    fun saveUser(user: User): Boolean

    fun updataUser(user: User): Boolean

}