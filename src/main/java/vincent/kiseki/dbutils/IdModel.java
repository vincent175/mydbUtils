package vincent.kiseki.dbutils;

public interface IdModel<T> {
    public T getId();

    public IdModel<T> setId(T id);

    public String getOrder();

    public IdModel<T> setOrder(String order);
}
