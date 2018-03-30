package vincent.kiseki.dbutils.anno;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * RetentionPolicy --
 * SOURCE  ：注解仅保存在源码中，在class字节码文件中不包含
 * CLASS   ：默认保留策略，注解会在class字节码文件中保存，但运行时无法获得
 * RUNTIME ：注解会在class字节码文件中保存，运行时可以通过
 * ElementType --
 * Type            ：接口，类，枚举，注解
 * FIELD           ：字段，枚举的常量
 * METHOD          ：方法
 * PARAMETER       ：方法参数
 * LOCAL_VARIABLE  ：局部变量
 * ANNOTATION_TYPE ：注解
 * PACKAGE         ：包
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface IdGenerator {

    String type() default "autoincrease";

    String param() default "";
}
