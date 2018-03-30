package vincent.kiseki.dbutils.jdbc;

import io.vertx.core.json.JsonArray;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter
@Setter
@ToString
@Accessors(chain = true)
public class SqlParam {

    private String sql;
    private JsonArray params;

    public SqlParam() {
    }
    public SqlParam(String sql, JsonArray params) {
        this.sql = sql;
        this.params = params;
    }
}
