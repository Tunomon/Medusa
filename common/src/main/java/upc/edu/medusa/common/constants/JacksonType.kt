package upc.edu.medusa.common.constants

import com.fasterxml.jackson.core.type.TypeReference

/**
 * Created by msi on 2017/5/8.
 */
object JacksonType {

    val LIST_OF_STRING: TypeReference<List<String>> = object : TypeReference<List<String>>() {

    }

    val LIST_OF_LONG: TypeReference<List<Long>> = object : TypeReference<List<Long>>() {

    }

    val MAP_OF_STRING: TypeReference<Map<String, String>> = object : TypeReference<Map<String, String>>() {

    }

    val MAP_OF_INTEGER: TypeReference<Map<String, Int>> = object : TypeReference<Map<String, Int>>() {

    }

    val MAP_OF_OBJECT: TypeReference<Map<String, Any>> = object : TypeReference<Map<String, Any>>() {

    }
}