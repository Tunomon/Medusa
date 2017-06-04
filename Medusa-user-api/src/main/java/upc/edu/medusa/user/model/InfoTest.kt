package upc.edu.medusa.user.model

import com.avaje.ebean.Model
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table

/**
 * Created by msi on 2017/4/14.
 */
@Entity
@Table(name = "info_test")
class InfoTest {

    companion object {
        val find = object : Model.Find<Long, InfoTest>() {}
    }

    // 主键Id
    @Id
    var id: Long? = null

    // 用户名
    var name: String = ""

    // 密码
    var sex: String = ""

    // 昵称
    var classs: String = ""

    // 邮箱
    var test_extra: String = ""


}