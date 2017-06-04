package upc.edu.medusa.common.util

/**
 * Created by msi on 2017/5/16.
 */
object Constants {


    // hive log message topic and type
    val HIVE_LOG_TOPIC = "hive_log_topic"
    val HIVE_LOG_MSG = 1

    // Oozie job process notify
    val OOZIE_JOB_PROCESS_TOPIC = "oozie_job_process_topic"
    val OOZIE_JOB_PROCESS_MSG = 2

    // hive 数据字典相关频道
    // hive 表不存在时创建相关表与列
    val HIVE_DATA_DICTIONARY_TOPIC = "hive_data_dictionary_topic"
    val HIVE_DATA_DICTIONARY_MSG = 3

    // hive 表相关频道
    // hive 创建血缘关系
    val HIVE_BLOOD_RELATION_TOPIC = "hive_blood_relation_topic"
    val HIVE_BLOOD_RELATION_MSG = 4

    val SELECT_REG = Regex("select\\s+", RegexOption.IGNORE_CASE)
    val CREATE_REG = Regex("create\\s+table\\s+", RegexOption.IGNORE_CASE)
    val INSERT_INTO_REG = Regex("insert\\s+into\\s+table\\s+", RegexOption.IGNORE_CASE)
    val DROP_REG = Regex("drop\\s+table\\s+", RegexOption.IGNORE_CASE)
    val ALTER_REG = Regex("alter\\s+table\\s+", RegexOption.IGNORE_CASE)
    val UPDATE_REG = Regex("update\\s+table\\s+", RegexOption.IGNORE_CASE)
    val TRUNCATE_REG = Regex("truncate\\s+table\\s+", RegexOption.IGNORE_CASE)
    val CREATE_DATABASE_REG = Regex("create\\s+database\\s+", RegexOption.IGNORE_CASE)
    val SELECT_ALL_REG = Regex("select\\s+\\*", RegexOption.IGNORE_CASE)
    val INSERT_OVERWRITE_REG = Regex("insert\\s+overwrite\\s+table\\s+", RegexOption.IGNORE_CASE)
    val LOCK_TABLE_REG = Regex("lock\\s+table\\s+", RegexOption.IGNORE_CASE)
    val UNLOCK_TABLE_REG = Regex("unlock\\s+table\\s+", RegexOption.IGNORE_CASE)

    val ADD_JAR_REG = Regex("add\\s+jar\\s+", RegexOption.IGNORE_CASE)
    val SET_HIVE_ARGS_REG = Regex("SET\\s+mapred*", RegexOption.IGNORE_CASE)
    val CREATE_FUNCTION_REG = Regex("create\\s+function*", RegexOption.IGNORE_CASE)
    val INVALIDATE_METADATA_REG = Regex("invalidate\\s+metadata*", RegexOption.IGNORE_CASE)

    val SELF_DATABASE = "self"
    val ORG_DATABASE = "org"
}