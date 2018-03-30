package vincent.kiseki.dbutils.meta;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.lang.annotation.Retention;

@Getter
@Setter
@Accessors(chain = true)
@ToString
public class Column {
    private String name;// efield.column()
    private String type;
    private String idType;
    private String idParam;
    private String refFName;
    private Integer length;
    private Integer precision;
    private Boolean pk;
    private Boolean fk;
    private Boolean index;
    private Boolean notNull;
    private String comment;
}
