import java.util.*;

class Database {
    Map<String, Table> tables;

    public Database() {
        tables = new HashMap<>();
    }

    public void createTable(Table table) {
        tables.put(table.tableName, table);
    }

    public Table getTable(String tableName) {
        return tables.get(tableName);
    }
}
