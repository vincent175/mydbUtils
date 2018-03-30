package vincent.kiseki.dbutils;

import vincent.kiseki.dbutils.anno.Entity;
import vincent.kiseki.dbutils.meta.Column;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 获取 sql执行器的工具类
 */
public class DB {
    public static final String ASYNC_SQL_DEFAULT_NAME = "async-sql-runner";
    public static final String SYNC_SQL_DEFAULT_NAME = "sync-sql-runner";
    public static final String ASYNC_REDIS_DEFAULT_NAME = "async-redis-runner";
    public static final String SYNC_REDIS_DEFAULT_NAME = "sync-redis-runner";

    public static Map<String, Object> runners = new HashMap<>();
    /**
     * String -- clazz.getName()
     * LinkedhashMap<String,Column>
     *     String -- class中field（属性）名称
     *     Column -- 注解中描述的column
     */
    public static Map<String, LinkedHashMap<String, Column>> fieldNameMappingColumn = new HashMap<>();
    /**
     * String -- clazz.getName()
     * linkedHashMap<String,Field>
     *     String -- 注解中column名称（当Efield注解没有添加column的信息，默认使用class的field的name）
     *     Field -- Class中field（属性）
     */
    public static Map<String, LinkedHashMap<String, Field>> columnNameMappingField = new HashMap<>();
    /**
     * String -- clazz.getName()
     * LinkedHashMap<String,Field>
     *     String -- Class中field（属性）名称
     *     Field -- Class中field（属性）
     */
    public static Map<String, LinkedHashMap<String, Field>> fieldNameMappingField = new HashMap<>();
    /**
     * String -- clazz.getName()
     * Entity -- Class上注解，对应db中的表
     */
    public static Map<String, Entity> tableMapping = new HashMap<>();

    public static


}
