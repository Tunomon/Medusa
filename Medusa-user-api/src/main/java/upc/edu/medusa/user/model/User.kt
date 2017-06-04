package upc.edu.medusa.user.model

import com.avaje.ebean.Model
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table

/**
 * Created by msi on 2017/4/14.
 */
@Entity
@Table(name = "medusa_user")
class User {

    companion object {
        val find = object : Model.Find<Long, User>() {}
    }

    // 主键Id
    @Id
    var id: Long? = null

    // 用户名
    var name: String = ""

    // 密码
    var password: String = ""

    // 昵称
    var nickName: String = ""

    // 邮箱
    var email: String = ""

    // 电话
    var phone: Long? = null

}