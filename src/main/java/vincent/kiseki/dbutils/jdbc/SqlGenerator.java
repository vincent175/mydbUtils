package vincent.kiseki.dbutils.jdbc;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;
import vincent.kiseki.dbutils.DB;
import vincent.kiseki.dbutils.IdModel;
import vincent.kiseki.dbutils.MarkDelete;
import vincent.kiseki.dbutils.anno.IdGenerator;
import vincent.kiseki.dbutils.meta.Column;
import vincent.kiseki.dbutils.utils.TypeUtils;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 生成sql语句
 */
public class SqlGenerator {
    /**
     * static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
     * SimpleDateFormat是线程不安全的，这里最好不要使用static的sdf（因为可能在多线程中同时访问到）
     */
    private static final ThreadLocal<SimpleDateFormat> df = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyyMMdd");
        }
    };
    private static SimpleDateFormat sdf = df.get();

    public static SqlParam getSql(IdModel<?> object) {
        return findSql(object, null, new String[]{"id"}, null, null, object.getClass());
    }

    /**
     * 根据Object对象，生成查询SqlParam
     * @param object 查询条件值
     * @param getFields 查询结果包含的字段
     * @param conditionFields 作为查询条件的字段
     * @param offset 查询结果offset
     * @param limit 查询结果limit
     * @param clazz
     * @return
     */
    public static SqlParam findSql(Object object, String[] getFields, String[] conditionFields, Integer offset, Integer limit, Class<?> clazz) {
        notAllNull(object, clazz);
        String className = object != null ? object.getClass().getName() : clazz.getName();
        LinkedHashMap<String, Column> cmapping = DB.fieldNameMappingColumn.get(className);
        LinkedHashMap<String, Field> rmapping = DB.fieldNameMappingField.get(className);
        if (cmapping == null || cmapping.isEmpty())
            throw new RuntimeException("Cann't find any mapping information for Class " + className);

        if (offset!=null&&offset<0) offset = 0;
        if (limit!=null&&limit<0) limit = 20;
        if (limit!=null&&offset==null) offset = 0; //当limit不为null的时候确保offset不为null

        List<String> getFieldsList = getFields != null ? Arrays.asList(getFields) : null;
        List<String> conditionList = conditionFields != null ? Arrays.asList(conditionFields) : null;
        StringBuffer getFieldsSql = new StringBuffer("");
        StringBuffer conditionSql = new StringBuffer(" 1=1 ");
        JsonArray params = new JsonArray();
        cmapping.forEach((fieldName,column) -> {
            Field field = rmapping.get(fieldName);
            if (getFieldsList == null || getFieldsList.contains(fieldName))
                getFieldsSql.append("t." + column.getName() + " as " + fieldName + ",");
            if (object != null && conditionList != null && !conditionList.isEmpty()) {
                if (conditionList.contains(fieldName)) {
                    if (addValueToParams(object, field, column, params)) conditionSql.append(" and t." + column.getName() + " = ? ");
                }else if (conditionList.contains(TypeUtils.attributeToUp(fieldName))) {
                    if (addValueToParams(object, rmapping.get(TypeUtils.attributeToUp(fieldName)), column, params)) conditionSql.append(" and t." + column.getName() + " < ? ");
                }else if (conditionList.contains(TypeUtils.attributeToDown(fieldName))) {
                    if (addValueToParams(object, rmapping.get(TypeUtils.attributeToDown(fieldName)), column, params)) conditionSql.append(" and t." + column.getName() + " >= ? ");
                } else if (conditionList.contains(fieldName + "#like")) {
                    if (addLikeValueToParams(object, field,column,params)) conditionSql.append(" and t." + column.getName() + " like ? ");
                } else {
                    StringBuffer likeConditionSql = new StringBuffer("");
                    conditionList
                        .stream()
                        .filter(conditionField -> conditionField.startsWith(fieldName + "#like(") && conditionField.endsWith(")"))
                        .findAny()
                        .ifPresent(likeField -> {
                            likeField = likeField.substring(likeField.indexOf("(") + 1, likeField.length() - 1);
                            List<String> likeConditionList = Arrays.asList(likeField.split(","));
                            cmapping.entrySet().stream()
                                .filter(e -> likeConditionList.contains(e.getKey()))
                                .filter(e -> addLikeValueToParams(object, field, column, params))
                                .forEach(e -> {likeConditionSql.append(" t." + e.getValue().getName() + " LIKE ? OR");});
                        });
                    if (likeConditionSql.length() > 0 && likeConditionSql.lastIndexOf("OR") > 0)
                        conditionSql.append(likeConditionSql.substring(0, likeConditionSql.lastIndexOf("OR")));
                }
            }
        });

        if (StringUtils.isBlank(getFieldsSql)) getFieldsSql.append(" * ");
        else getFieldsSql.deleteCharAt(getFieldsSql.length() - 1); //去掉拼接sql时多余的最后一个逗号（,）

        String sql = "select " + getFieldsSql.toString() + " from "
            + DB.tableMapping.get(className).table() + " t where " + conditionSql.toString();

        if (clazz!=null && MarkDelete.class.isAssignableFrom(clazz))
            sql = sql + " and isDelete = false";
        else if(MarkDelete.class.isAssignableFrom(object.getClass()))
            sql = sql + " and isDelete = false";

        if (object != null && object instanceof IdModel && StringUtils.isNotBlank(((IdModel) object).getOrder()))
            sql = sql + " order by " + ((IdModel) object).getOrder();

        if (offset != null) {
            sql = sql + " limit ?";
            params.add(offset);
        }
        if (limit != null && limit != 0) {
            sql = sql + ",?";
            params.add(limit);
        }
        return new SqlParam(sql, params);
    }


    /**
     * 根据Object对象，生成插入SqlParam
     * @param object 插入对象，可以是集合
     * @return
     */
    public static SqlParam insertSql(Object object) {
        Objects.requireNonNull(object, "insert value cannot be null");
        JsonArray params = new JsonArray();
        StringBuffer insertFields = new StringBuffer();
        StringBuffer insertValues = new StringBuffer();
        String className = null;
        if (object instanceof Collection) {
            if (((Collection) object).isEmpty()) throw new RuntimeException("Cann't insert an empty Collection");
            Map<String, Column> cmapping = null;
            Map<String, Field> rmapping = null;
            Iterator iterator = ((Collection) object).iterator();
            Boolean firstTime = Boolean.TRUE; //是否为第一次生成 inset sql（只需要生成一次即可）
            while (iterator.hasNext()) {
                Object value = iterator.next();
                if (firstTime) {
                    className = value.getClass().getName();
                    cmapping = DB.fieldNameMappingColumn.get(className);
                    rmapping = DB.fieldNameMappingField.get(className);
                }
                if (cmapping == null || cmapping.isEmpty())
                    throw new RuntimeException("Cann't find any mapping information for Class " + object.getClass().getName());

                JsonArray oneData = new JsonArray();
                Iterator<String> fIterator = cmapping.keySet().iterator();
                while (fIterator.hasNext()) {
                    String fieldName = fIterator.next();
                    Column column = cmapping.get(fieldName);
                    try {
                        Field field = rmapping.get(fieldName);
                        IdGenerator idGenerator = field.getAnnotation(IdGenerator.class);
                        if (column.getPk() && idGenerator != null) {
                            field.setAccessible(true);
                            switch (idGenerator.type().toLowerCase()) {
                                case "assigned":
                                    if (field.get(value) == null) oneData.addNull();
                                    else oneData.add(field.get(value));
                                    if (firstTime) {
                                        insertFields.append(column.getName() + ",");
                                        insertValues.append("?,");
                                    }
                                    break;
                                case "sequence":
                                    if (firstTime) {
                                        insertFields.append(column.getName() + ",");
                                        insertValues.append(idGenerator.param() + ".nextVal()");
                                    }
                                    break;
                                case "uuid":
                                    field.set(value, UUID.randomUUID().toString());
                                    oneData.add(field.get(value));
                                    if (firstTime) {
                                        insertFields.append(column.getName() + ",");
                                        insertValues.append("?,");
                                    }
                                    break;
                                case "autoincrease":
                                    break;
                                default:
                                    throw new RuntimeException(" Donn't support pk type,value must in (assigned|sequence|uuid|autoincrease)");
                            }
                        } else {
                            addValueToParamsIncludingNull(value,field,column,oneData);
                            if (firstTime) {
                                insertFields.append(column.getName() + ",");
                                insertValues.append("?,");
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Cann't get value of field " + value.getClass().getName() + "." + fieldName);
                    }
                }
                params.add(oneData);
                firstTime = Boolean.FALSE;
            }
        }else {
            final String key;
            className = key = object.getClass().getName();
            final Map<String, Column>  cmapping = DB.fieldNameMappingColumn.get(key);
            final Map<String, Field> rmapping = DB.fieldNameMappingField.get(key);
            if (cmapping == null || cmapping.isEmpty())
                throw new RuntimeException("Cann't find any mapping information for Class " + key);
            cmapping.entrySet().forEach(entry -> {
                String fieldName = entry.getKey();
                Column column = entry.getValue();
                try {
                    Field field = rmapping.get(fieldName);
                    IdGenerator idGenerator = field.getAnnotation(IdGenerator.class);
                    field.setAccessible(true);
                    if (column.getPk() && idGenerator != null) {
                        switch (idGenerator.type().toLowerCase()) {
                            case "assigned": //指定id
                                if (field.get(object)==null) params.addNull();
                                else params.add(field.get(object));
                                insertFields.append(column.getName() + ",");
                                insertValues.append("?,");
                                break;
                            case "sequence"://指定序列
                                insertFields.append(column.getName() + ",");
                                insertValues.append(idGenerator.param() + ".nextVal");
                                break;
                            case "uuid"://UUID
                                insertFields.append(column.getName() + ",");
                                insertValues.append(UUID.randomUUID().toString());
                                break;
                            case "autoincrease":
                                break;
                            default:
                                throw new RuntimeException("Don't support this pk type,value must in (assigned|sequence|uuid|autoincrease)");
                        }
                    }else {
                        addValueToParamsIncludingNull(object,field,column,params);
                        insertFields.append(column.getName() + ",");
                        insertValues.append("?,");
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Cann't get value of field " + key + "." + fieldName);
                }
            });
        }
        if (StringUtils.isNotBlank(insertFields)) insertFields.deleteCharAt(insertFields.length() - 1);
        else throw new RuntimeException("Generate insert SQL failed,beacause there is none insertFields");
        if (StringUtils.isNotBlank(insertValues)) insertValues.deleteCharAt(insertValues.length() - 1);
        else throw new RuntimeException("Generate insert SQL failed,beacause there is none insertValues");
        String sql = "insert into " + DB.tableMapping.get(className).table()
            + "(" + insertFields + ") values (" + insertValues + ")";
        return new SqlParam(sql, params);
    }

    /**
     * 根据Object对象，生成更新 SqlParam
     * @param object 更新的对象
     * @param updateFields obj里待更新的字段
     * @param conditionFields obj作为更新条件的字段
     * @return
     */
    public static SqlParam updateSql(Object object, String[] updateFields, String[] conditionFields) {
        Objects.requireNonNull(object, "update object can not be null");
        String className = object.getClass().getName();
        LinkedHashMap<String, Column> cmapping = DB.fieldNameMappingColumn.get(className);
        LinkedHashMap<String, Field> rmapping = DB.fieldNameMappingField.get(className);

        if (cmapping==null||cmapping.isEmpty())
            throw new RuntimeException("Cann't find any mapping information from Class " + className);

        StringBuffer updateFieldsSql = new StringBuffer("");
        StringBuffer updateConditionSql = new StringBuffer(" 1=1 ");
        List<String> updateFieldList = updateFields != null ? Arrays.asList(updateFields) : null;
        List<String> updateConditionList = conditionFields != null ? Arrays.asList(conditionFields) : null;
        JsonArray params = new JsonArray();
        JsonArray conditionValue = new JsonArray();

        cmapping.forEach((fieldName,column) -> {
            if (updateFieldList != null && !updateFieldList.isEmpty() && updateFieldList.contains(fieldName)) {
                updateFieldsSql.append(" t." + column.getName() + " = ? ,");
                addValueToParamsIncludingNull(object,rmapping.get(fieldName),column,params);
            }
        });
        cmapping.forEach((fieldName,column)->{
            Field field = rmapping.get(fieldName);
            if (updateConditionList == null || updateConditionList.contains(fieldName)) {
                if (addValueToParams(object,field,column,conditionValue))
                    updateConditionSql.append(" and t." + column.getName() + " = ? ");
            } else if (updateConditionList.contains(TypeUtils.attributeToUp(fieldName))) {
                if (addValueToParams(object,rmapping.get(TypeUtils.attributeToUp(fieldName)),column,conditionValue))
                    updateConditionSql.append(" and t." + column.getName() + " < ? ");
            } else if (updateConditionList.contains(TypeUtils.attributeToDown(fieldName))) {
                if (addValueToParams(object,rmapping.get(TypeUtils.attributeToDown(fieldName)),column,conditionValue))
                    updateConditionSql.append(" and t." + column.getName() + " > ? ");
            } else if (updateConditionList.contains(fieldName + "#like")) {
                if (addLikeValueToParams(object,field,column,conditionValue))
                    updateConditionSql.append(" and t." + column.getName() + " like ?");
            }else {
                StringBuffer likeConditionSql = new StringBuffer("");
                updateConditionList.stream()
                    .filter(conditionField -> conditionField.startsWith(fieldName + "#like(")&&conditionField.endsWith(")"))
                    .findAny()
                    .ifPresent(likeFieldsStr -> {
                        likeFieldsStr = likeFieldsStr.substring(likeFieldsStr.indexOf("("), likeFieldsStr.length() - 1);
                        List<String> likeConditionList = Arrays.asList(likeFieldsStr.split(","));
                        cmapping.entrySet().stream()
                            .filter(e -> likeConditionList.contains(e.getKey()))
                            .forEach(e -> {
                                if (addLikeValueToParams(object,field,column,conditionValue))
                                    likeConditionSql.append(" t." + column.getName() + " like ? or ");
                            });
                    });
                if (likeConditionSql.length()>0)
                    updateConditionSql.append("and (" + likeConditionSql.substring(0, likeConditionSql.lastIndexOf("or")) + ")");
            }
        });
        params.addAll(conditionValue);
        String sql = "update " + DB.tableMapping.get(className) + " t set "
            + updateFieldsSql.substring(0,updateFieldsSql.length()-1)
            + " where " + updateConditionSql.toString();
        return new SqlParam(sql, params);
    }

    public static SqlParam updateSql(IdModel object){
        return updateSql(object, null, new String[]{"id"});
    }

    public static SqlParam updateSql(IdModel object, String[] updateFields) {
        return updateSql(object, updateFields, new String[]{"id"});
    }

    /**
     *
     * @param object 被删对象
     * @param conditionFields object作为被删条件的字段
     * @return
     */
    public static SqlParam deleteSql(Object object, String[] conditionFields) {
        Objects.requireNonNull(object, "be deleted object(s) cannot be null");
        String className = object.getClass().getName();
        Map<String, Column> cmapping = DB.fieldNameMappingColumn.get(className);
        Map<String, Field> rmapping = DB.fieldNameMappingField.get(className);

        if (cmapping==null||cmapping.isEmpty())
            throw new RuntimeException("cannot find any mapping information for class " + className);

        List<String> conditionList = conditionFields != null ? Arrays.asList(conditionFields) : null;
        StringBuffer conditionSql = new StringBuffer(" 1=1 ");
        JsonArray params = new JsonArray();
        cmapping.forEach((fieldName,column) -> {
            Field field = rmapping.get(fieldName);
            if (conditionList == null || conditionList.contains(fieldName)) {
                if (addValueToParams(object, field, column, params))
                    conditionSql.append(" and t." + column.getName() + " = ? ");
            } else if (conditionList.contains(TypeUtils.attributeToDown(fieldName))) {
                if (addValueToParams(object,rmapping.get(TypeUtils.attributeToDown(fieldName)),column,params))
                    conditionSql.append(" and t. " + column.getName() + " >= ? ");
            } else if (conditionList.contains(TypeUtils.attributeToUp(fieldName))) {
                if (addValueToParams(object,rmapping.get(TypeUtils.attributeToUp(fieldName)),column,params))
                    conditionSql.append(" and t. " + column.getName() + " < ? ");
            } else if (conditionList.contains(fieldName + "#like")) {
                if (addLikeValueToParams(object, field, column, params))
                    conditionSql.append(" and t." + column.getName() + " like ? ");
            } else {
                StringBuffer likeSql = new StringBuffer("");
                conditionList.stream()
                    .filter(fName -> fName.startsWith(fieldName + "#like(")&&fName.endsWith(")"))
                    .findAny()
                    .ifPresent(likeStr -> {
                        likeStr = likeStr.substring(likeStr.indexOf("("), likeStr.length() - 1);
                        List<String> likeConditionList = Arrays.asList(likeStr.split(","));
                        cmapping.entrySet().stream()
                            .filter(e -> likeConditionList.contains(e.getKey()))
                            .forEach(e -> {
                                if (addLikeValueToParams(object,rmapping.get(fieldName),column,params))
                                    likeSql.append(" t." + e.getValue().getName() + " like ? or");
                            });
                    });
                if (likeSql.length()>0)
                    conditionSql.append(" and (" + likeSql.substring(0, likeSql.lastIndexOf("or")) + ")");
            }
        });
        if (conditionSql.toString().equals(" 1=1 "))
            conditionSql.append(" and 1=0 ");
        String sql = "",tableName = DB.tableMapping.get(className).table();
        if (MarkDelete.class.isAssignableFrom(object.getClass()))
            sql = "update " + tableName + " t set isDelete = true " + conditionSql.toString() + " and isDelete = false";
        else
            sql = "delete from " + tableName + " t where " + conditionSql.toString();

        return new SqlParam(sql, params);
    }

    /**
     *
     * @param object 查询条件从object中取值
     * @param conditionFields 作为查询条件的字段
     * @param clazz 这个参数好像没什么作用？？？留着做扩展？？？
     * @return
     */
    public static SqlParam countSql(Object object, String[] conditionFields) {
        Objects.requireNonNull(object, "count value cannot be null");
        String className = object.getClass().getName();
        LinkedHashMap<String, Column> cmapping = DB.fieldNameMappingColumn.get(className);
        LinkedHashMap<String, Field> rmapping = DB.fieldNameMappingField.get(className);
        if (cmapping==null || cmapping.isEmpty())
            throw new RuntimeException("cannot find any mapping information for Class " + className);

        List<String> conditionList = conditionFields != null ? Arrays.asList(conditionFields) : null;
        StringBuffer conditionSql = new StringBuffer(" 1=1 ");
        JsonArray params = new JsonArray();

        if (conditionList != null && !conditionList.isEmpty()) {
            cmapping.forEach((fieldName, column) -> {
                Field field = rmapping.get(fieldName);
                if (conditionList.contains(fieldName))
                    if (addValueToParams(object,field,column,params))
                        conditionSql.append(" and t." + column.getName() + " = ? ");
                else if (conditionList.contains(TypeUtils.attributeToUp(fieldName)))
                    if (addValueToParams(object,rmapping.get(TypeUtils.attributeToUp(fieldName)),column,params))
                        conditionSql.append(" and t." + column.getName() + " < ? ");
                else if (conditionList.contains(TypeUtils.attributeToDown(fieldName)))
                    if (addValueToParams(object,rmapping.get(TypeUtils.attributeToDown(fieldName)),column,params))
                        conditionSql.append(" and t." + column.getName() + " >= ? ");
                else if (conditionList.contains(fieldName + "#like"))
                    if (addValueToParams(object,field,column,params))
                        conditionSql.append(" and t." + column.getName() + " like ? ");
                else {
                        StringBuffer likeSql = new StringBuffer("");
                        conditionList.stream()
                            .filter(conditionStr -> conditionStr.startsWith(fieldName + "#like(") && conditionStr.endsWith(")"))
                            .findAny()
                            .ifPresent(likeStr -> {
                                likeStr = likeStr.substring(likeStr.indexOf("("), likeStr.length() - 1);
                                List<String> likeConditionList = Arrays.asList(likeStr.split(","));
                                cmapping.entrySet().stream()
                                    .filter(e -> likeConditionList.contains(e.getKey()))
                                    .forEach(e -> {
                                        if (addLikeValueToParams(object,rmapping.get(fieldName),column,params))
                                            likeSql.append(" t." + e.getValue().getName() + " like ? or");
                                    });
                            });
                        if (likeSql.length()>0)
                            conditionSql.append(" and (" + likeSql.substring(0, likeSql.lastIndexOf("or")) + ")");
                    }
            });
        }
        String sql = "select count(1) as cnt from " + DB.tableMapping.get(className).table() + " t where " + conditionSql;
        return new SqlParam(sql, params);
    }

    /**
     * 判断表是否存在
     * @param tableName 表名称
     * @param tableSchema 数据库名称
     * @param db 数据库类型
     * @return
     */
    public static SqlParam existTable(String tableName, String tableSchema, String db) {
        SqlParam sqlParam = new SqlParam();
        switch (db) {
            case "mysql":
                sqlParam.setSql("select count(1) from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ? and TABLE_NAME = ?");
                sqlParam.setParams(new JsonArray().add(tableSchema).add(tableName));
                break;
            case "postgre":
                //TODO 增加Postgre数据库支持
                break;
            default:
                break;
        }
        return sqlParam;
    }

    /**
     *
     * @param tableName 表名称
     * @param tableSchema 数据库名称
     * @param tableComment 表描述
     * @param columns 表所包含的列
     * @param db 数据库类型
     * @return
     */
    public static SqlParam createTable(String tableName, String tableSchema, String tableComment, Collection<Column> columns, String db) {
        SqlParam sqlParam = new SqlParam();
        StringBuffer createSql = new StringBuffer("create table if not exists ");
        if (StringUtils.isNotBlank(tableSchema)) createSql.append(tableSchema).append(".");
        createSql.append(tableName).append("(");
        switch (db) {
            case "mysql":
                columns.forEach(column -> {
                    createSql.append(column.getName()).append(" ").append(convert2DBType(db, column.getType()))
                        .append(conver2DBPrecision(db, column.getType(), column.getLength(), column.getPrecision()))
                        .append(column.getNotNull() || column.getPk() ? " not null " : " default null")
                        .append(" comment '").append(column.getComment()).append("'")
                        .append(column.getPk() && column.getIdType() != null && column.getIdType().equalsIgnoreCase("autoincrease") ? " auto_increment" : "");
                    if (column.getPk()) createSql.append(",primary key (").append(column.getName()).append(")");
                    if (column.getIndex())
                        createSql.append(",key ").append("index_")
                            .append("" + System.currentTimeMillis() + (int) (Math.random() * 100))
                            .append("(").append(column.getName()).append(")");
                    createSql.append(",");
                });
                break;
            case "postgre":
                //TODO 增加Postgre数据支持
                break;
            default:
                break;
        }
        String sql = createSql.substring(0, createSql.length() - 1) + ")";
        if (StringUtils.isNotBlank(tableComment)) sql += " comment='" + tableComment + "'";
        sqlParam.setSql(sql);
        return sqlParam;
    }

    /**
     * 查询表包含的所有字段名称
     * @param tableName 表名称
     * @param tableSchema 数据库名称
     * @param db 数据库类型
     * @return
     */
    public static SqlParam existColumn(String tableName, String tableSchema, String db) {
        SqlParam sqlParam = new SqlParam();
        switch (db) {
            case "mysql":
                sqlParam.setSql("select column_name from information_schema.columns where table_schema = ? and table_name = ?");
                sqlParam.setParams(new JsonArray().add(tableSchema).add(tableName));
                break;
            default:
                break;
        }
        return sqlParam;
    }

    /**
     * 向表tableName中插入column
     * @param tableName 表名
     * @param tableSchema 数据库名
     * @param column 列
     * @param db 数据库类型
     * @return
     */
    public static SqlParam addColumn(String tableName, String tableSchema, Column column, String db) {
        SqlParam sqlParam = new SqlParam();
        StringBuffer createSql = new StringBuffer();
        switch (db) {
            case "mysql":
                createSql.append("alter table ");
                if (StringUtils.isNotBlank(tableSchema)) createSql.append(tableSchema).append(".");
                createSql.append(tableName).append(" add ")
                    .append(column.getName()).append(" ").append(convert2DBType(db, column.getType()))
                    .append(conver2DBPrecision(db, column.getType(), column.getLength(), column.getPrecision()))
                    .append(column.getPk() ? " primary key " : "")
                    .append(!column.getNotNull() && !column.getPk() ? " default null " : " not null ")
                    .append(column.getPk() && column.getIdType() != null && column.getIdType().equalsIgnoreCase("autoincrease") ? " auto_increment " : "");
                sqlParam.setSql(createSql.toString());
                break;
            default:
                break;
        }
        return sqlParam;
    }

    private static String convert2DBType(String db, String type) {
        switch (db) {
            case "mysql":
                switch (type.toLowerCase()) {
                    case "string":
                        return "VARCHAR";
                    case "int":
                        return "INT";
                    case "integet":
                        return "INT";
                    case "long":
                        return "INT";
                    case "short":
                        return "INT";
                    case "boolean":
                        return "INT(1)";
                    case "double":
                        return "DOUBLE";
                    case "float":
                        return "FLOAT";
                    case "date":
                        return "DATETIME";
                    default:
                        return type;
                }
            default:
                return type;
        }
    }

    private static String conver2DBPrecision(String db, String type, Integer length, Integer precision) {
        switch (db) {
            case "mysql":
                switch (type.toLowerCase()) {
                    case "string":
                        if (length==null) return "(255)";
                        else return "(" + length + ")";
                    case "int":
                        if (length==null) return "(8)";
                        else return "(" + length + ")";
                    case "integer":
                        if (length==null) return "(8)";
                        else return "(" + length + ")";
                    case "long":
                        if (length==null) return "(11)";
                        else return "(" + length + ")";
                    case "short":
                        if (length==null) return "(5)";
                        else return "(" + length + ")";
                    case "boolean":
                        return "";
                    case "float":
                        if (length==null) return "(8,2)";
                        else return "(" + length + "," + precision + ")";
                    case "date":
                        return "";
                    default:
                        return "";
                }
            default:
                return "";
        }
    }

    /**
     * 将Object中field的值存到params中,
     * 当column为fk时，取object中的field中的名称为column的refFName的属性
     * @param object 从object中取值
     * @param field 要取值的属性
     * @param column 数据库字段相关信息
     * @param params 取值保存到该数组中
     */
    private static void addValueToParamsIncludingNull(Object object,Field field,Column column,JsonArray params) {
        try {
            field.setAccessible(true);
            Object value = field.get(object);
            if (value == null) {
                params.addNull();
                return;
            }
            if (Boolean.class.isAssignableFrom(field.getType())) params.add((boolean) field.get(object) ? 1 : 0);
            else if (Date.class.isAssignableFrom(field.getType())) params.add(sdf.format((Date) field.get(object)));
            else if (column.getFk()) {
                //Efield的 refFName 的 default值为 "id"
                Object paramValue = invoke(field.get(object), column.getRefFName());
                if (paramValue == null) params.addNull();
                else params.add(paramValue);
            }else
                params.add(field.get(object));

        } catch (Exception e) {
            throw new RuntimeException("Cann't get value of field " + object.getClass() + "." + field.getName());
        }

    }

    private static Boolean addValueToParams(Object obj, Field field, Column column, JsonArray params) {
        try {
            field.setAccessible(true);
            Object value = field.get(obj);
            if (value == null) return false;
            if (value instanceof String && StringUtils.isBlank((String)value)) return false;
            if (Boolean.class.isAssignableFrom(field.getType())) params.add((boolean) field.get(obj) ? 1 : 0);
            else if (Date.class.isAssignableFrom(field.getType())) params.add(sdf.format((Date) field.get(obj)));
            //外键的判断，下面是ChargeApp中的字段CmChannel，ChargeApp的外键（fk）-cc_id 是 CmChannel中的主键（pk） -- id
            //@Efield(column="cc_id",type="VARCHAR(32)",length=5,fk=true,refFName="channelCode")
            //private CmChannel cmChannel;
            //refFName="channelCode" 是 CmChannel中的字段名称
            else if (column.getFk()) {
                Object paramValue = invoke(field.get(obj), column.getRefFName());
                if (paramValue==null) return false;
                else params.add(paramValue);
            }else {
                params.add(field.get(obj));
            }
            return true;
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Cann't get value of field " + obj.getClass() + "." + field.getName());
        }
    }

    private static Boolean addLikeValueToParams(Object obj, Field field, Column column, JsonArray params) {
        try{
            field.setAccessible(true);
            Object value = field.get(obj);
            if (value == null) return false;
            if (value instanceof String && StringUtils.isBlank((String)value)) return false;
            if (Boolean.class.isAssignableFrom(field.getType())) return false;
            else if (Date.class.isAssignableFrom(field.getType()))
                params.add("%" + sdf.format((Date) field.get(obj)) + "%");
            else if (column.getFk()){
                Object paramValue = invoke(field.get(obj),column.getRefFName());
                if(paramValue==null) return false;
                else params.add("%" + paramValue + "%");
            }else {
                params.add("%" + field.get(obj) + "%");
            }
            return true;
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException("Cann't get value of field " + obj.getClass() + "." + field.getName());
        }
    }

    private static Object invoke(Object object, String fieldname) throws NoSuchFieldException, IllegalAccessException {
        Field field = object.getClass().getDeclaredField(fieldname);
        field.setAccessible(true);
        return field.get(object);
    }

    private static void notAllNull(Object obj1, Object obj2) {
        if (obj1 == null && obj2 == null) throw new RuntimeException("params cann't be both null");
    }
}
