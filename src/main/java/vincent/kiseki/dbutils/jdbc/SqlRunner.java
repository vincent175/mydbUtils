package vincent.kiseki.dbutils.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import vincent.kiseki.dbutils.DB;
import vincent.kiseki.dbutils.IdModel;
import vincent.kiseki.dbutils.meta.Column;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class SqlRunner {
    private HikariDataSource dataSource;
    private String db;
    private Boolean showSql;

    // 根据配置初始化数据源
    public SqlRunner(JsonObject jsonObject) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jsonObject.getString("jdbcUrl"));
        if (StringUtils.isNotBlank(jsonObject.getString("driverClassName")))
            config.setDriverClassName(jsonObject.getString("driverClassName"));
        if (StringUtils.isNotBlank(jsonObject.getString("username")))
            config.setUsername(jsonObject.getString("username"));
        if (StringUtils.isNotBlank(jsonObject.getString("password")))
            config.setPassword(jsonObject.getString("password"));
        // 等待连接池分配连接的最大时长(毫秒),超过这个时长还没可用的连接则发生SQLException , 缺省:30秒
        if (jsonObject.getLong("connectTimeout") != null)
            config.setConnectionTimeout(jsonObject.getLong("connectTimeout"));
        // 设置空闲时间 , 一个连接idle状态的最大时长(毫秒) , 超时则被释放 , 缺省:10分钟
        if (jsonObject.getLong("idleTimeout") != null)
            config.setIdleTimeout(jsonObject.getLong("idleTimeout"));
        // 设置连接池最大连接数 , 缺省值: 10
        if (jsonObject.getInteger("syncMaxPoolSize") != null)
            config.setMaximumPoolSize(jsonObject.getInteger("syncMaxPoolSize"));
        // 设置连接的生命时长 , 超时而且没有被使用则被释放 , 缺省:30分钟 , 建议设置比数据库超时时长少30秒
        if (jsonObject.getInteger("maxLifetime") != null)
            config.setMaxLifetime(jsonObject.getInteger("maxLifetime"));
        // 连接只读数据库时配置为 true , 保证安全
        if (jsonObject.getBoolean("readOnly"))
            config.setReadOnly(jsonObject.getBoolean("readOnly"));
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit","2048");
        dataSource = new HikariDataSource(config);
        showSql = jsonObject.getBoolean("showSql");
    }

    /**
     * 根据T（实现了IdModel）的id来查询对象
     * @param t 查询条件
     * @param <T> 返回查询结果T的实例
     * @return
     */
    public <T extends IdModel<?>> T get(T t){
        Objects.requireNonNull(t,"the case of IdModel cannot be null");
        SqlParam sqlParam = SqlGenerator.getSql(t);
        return ;
    }

    public <T> T getT(String sql, JsonArray params, Class<T> clazz) {
        String className = clazz.getName();
        Map<String, Field> fmapping = DB.columnNameMappingField.get(className);
        Map<String, Column> cmapping = DB.fieldNameMappingColumn.get(className);
        Map<String, Field> rmapping = DB.fieldNameMappingField.get(className);

    }


    /**
     * 同步获取单条数据
     * @param sql SQL语句
     * @param params 查询参数
     * @param mapper
     * @param <T>
     * @return
     */
    public <T> T get(String sql, JsonArray params, RowMapper<T> mapper) {
        printSql(sql,params);
        Connection connection = getConnection();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            Optional.ofNullable(params).ifPresent( -> {

            });

        } catch (SQLException e) {
            e.printStackTrace();
            //TODO 日记记录
            return null;
        }finally {
            try {
                releaseConnection(connection);
            } catch (SQLException e) {
                e.printStackTrace();
                //TODO 日记记录
                throw new RuntimeException(e);
            }
        }
    }

    private static final ThreadLocal<Connection> connInThreadLocal = new ThreadLocal<>();

    /**
     * 从当前线程中获取数据库可用连接，如果没有从连接池中获取并保存到当前线程中
     * @return
     */
    public Connection getConnection(){
        try {
            Connection connection = connInThreadLocal.get();
            if (connection == null || connection.isClosed()) {
                connection = dataSource.getConnection();
                connInThreadLocal.set(connection);
            }
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 开启事务
     */
    public void beginTransaction(){
        try {
            getConnection().setAutoCommit(Boolean.FALSE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 提交事务
     */
    public void commit() {
        Connection connection = getConnection();
        try {
            if (connection != null && !connection.isClosed() && connection.getAutoCommit()) {
                connection.commit();
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void releaseConnection(Connection connection) throws SQLException {
        if (connection != null && !connection.isClosed() && connection.getAutoCommit()) {
            connection.close();
        }
    }

    private void printSql(String sql, JsonArray params) {
        if (showSql!=null&&showSql){
            //TODO logger.info(sql + "," + params) 日记记录sql
        }
    }



}
