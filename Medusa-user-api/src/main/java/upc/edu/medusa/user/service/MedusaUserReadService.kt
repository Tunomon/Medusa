package upc.edu.medusa.user.service

import upc.edu.medusa.user.model.User


/**
 * Created by msi on 2017/4/14.
 */


interface MedusaUserReadService{

    fun findUserByName(name : String): User?

}