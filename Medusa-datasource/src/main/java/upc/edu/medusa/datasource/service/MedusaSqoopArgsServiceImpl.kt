package upc.edu.medusa.datasource.service

import com.google.common.base.Joiner
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.springframework.stereotype.Service
import upc.edu.medusa.common.util.RandomStringUtil
import upc.edu.medusa.datasource.model.MedusaDatasource
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.COMPRESS
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.CONNECT
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.DEFAULT_FIELDS_TERMINATED_BY
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.DEFAULT_INPUT_FIELDS_TERMINATED_BY
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.DEFAULT_INPUT_NULL_NON_STRING
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.DEFAULT_INPUT_NULL_STRING
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.DEFAULT_NULL_NON_STRING
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.DEFAULT_NULL_STRING
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.DELETE_TARGET_DIR
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.DROP_IMPORT_DELIMS
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.EXPORT
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.EXPORT_DIR
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.FIELDS_TERMINATED_BY
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.HIVE_IMPORT
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.HIVE_OVERWRITE
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.HIVE_PARTITION_KEY
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.HIVE_PARTITION_VALUES
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.HIVE_TABLE
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.IMPORT
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.INPUT_FIELDS_TERMINATED_BY
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.INPUT_NULL_NON_STRING
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.INPUT_NULL_STRING
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.MAPPERS
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.NULL_NON_STRING
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.NULL_STRING
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.PASSWORD
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.RECURSIVE_EXPORT
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.TABLE
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.TARGET_DIR
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.UPDATE_MODE
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.UPDATE_MODE_INSERT
import upc.edu.medusa.datasource.model.SqoopArgs.Companion.USERNAME


/**
 * Created by msi on 2017/5/14.
 */

@Service
class MedusaSqoopArgsServiceImpl : MedusaSqoopArgsService{




    override fun getImportAllTablesToHiveSqoopCommandArgs(datasource: MedusaDatasource,  table: String, extra: Map<String, String>): String {
        val args = mutableListOf<String>()
        val partition_key: String = extra["partitionKey"]!!
        val partition_value: String = extra["partitionValue"]!!
        val overwrite: String = extra["overwrite"]!!
        val importHiveTable: String = extra["importHiveTable"]!!
        val (database, tableName) = importHiveTable.split(".")
        args.add(IMPORT )
        args.add(CONNECT)
        args.add(datasource.url!!)
        args.add(USERNAME)
        args.add(datasource.dbUsername!!)
        args.add(PASSWORD)
        args.add(datasource.dbPassword!!)
        args.add(HIVE_TABLE)
        args.add(importHiveTable)
        args.add(HIVE_IMPORT)
        args.add(FIELDS_TERMINATED_BY)
        args.add(DEFAULT_FIELDS_TERMINATED_BY)
        args.add(NULL_STRING)
        args.add(DEFAULT_NULL_STRING)
        args.add(NULL_NON_STRING)
        args.add(DEFAULT_NULL_NON_STRING)
        args.add(MAPPERS)
        args.add("1")
        args.add(DROP_IMPORT_DELIMS)
        args.add(HIVE_PARTITION_KEY)
        args.add(partition_key)
        args.add(HIVE_PARTITION_VALUES)
        args.add(partition_value)
        args.add(COMPRESS)
        args.add(DELETE_TARGET_DIR)
        args.add(TARGET_DIR)
        args.add("sqoop/$database/${table}To$tableName.${DateTimeFormat.forPattern("yyyyMMddHHmmss").print(DateTime.now())}.${RandomStringUtil.createRandomString(6)}")
        if (overwrite == "on") {
            args.add(HIVE_OVERWRITE)
        }
        args.add(TABLE)
        args.add(table)
        return Joiner.on(' ').skipNulls().join(args)
    }

    override fun getExportAllTableSqoopCommandArg(datasource: MedusaDatasource, table: String, extra: Map<String, String>): String {
        val args = mutableListOf<String>()
        val location: String  = extra["location"]!!
        args.add(EXPORT)
        args.add(CONNECT)
        args.add(datasource.url!!)
        args.add(USERNAME)
        args.add(datasource.dbUsername!!)
        args.add(PASSWORD)
        args.add(datasource.dbPassword!!)
        args.add(TABLE)
        args.add(table)
        args.add(EXPORT_DIR)
        args.add(location)
        args.add(INPUT_FIELDS_TERMINATED_BY)
        args.add(DEFAULT_INPUT_FIELDS_TERMINATED_BY)
        args.add(INPUT_NULL_STRING)
        args.add(DEFAULT_INPUT_NULL_STRING)
        args.add(INPUT_NULL_NON_STRING)
        args.add(DEFAULT_INPUT_NULL_NON_STRING)
        args.add(UPDATE_MODE)
        args.add(UPDATE_MODE_INSERT)
        args.add(MAPPERS)
        args.add("1")
        args.add(RECURSIVE_EXPORT)

        return Joiner.on(' ').skipNulls().join(args)
    }
}