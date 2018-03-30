package vincent.kiseki.dbutils.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Efield {

    String column() default "";

    String key() default "";

    boolean index() default false;

    boolean pk() default false;

    boolean fk() default false;

    String type() default "";

    int length() default 0;

    int precision() default 0;// 精度

    boolean notNull() default false;

    String refFName() default "id";

}
