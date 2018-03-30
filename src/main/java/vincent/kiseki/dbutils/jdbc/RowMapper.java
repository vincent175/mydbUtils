package vincent.kiseki.dbutils.jdbc;

import java.sql.ResultSet;
import java.util.List;

@FunctionalInterface
public interface RowMapper<T> {

    /**
     * 将ResultSet转化为类型T
     * @param rs ResultSet（结果集）
     * @param columnLabels 列名集合（columnName是sql语句中field的原始名字，columnLabel是sql中as的值/alias-别名）
     * @return
     * @throws Exception
     */
    T map(ResultSet rs, List<String> columnLabels) throws Exception;
}
