import java.util.*;

public class Table {
	String tableName;
	List<Column> columns;
	List<Row> rows;

	public Table(String tableName) {
		this.tableName = tableName;
		this.columns = new ArrayList<>();
		this.rows = new ArrayList<>();
	}

	public void addColumn(Column column) {
		columns.add(column);
	}

	public void insert(List<String> columnNames, List<Object> values) {
		Row row = new Row();
		for (int i = 0; i < columnNames.size(); i++) {
			String columnName = columnNames.get(i);
			Object value = values.get(i);

			for (Column col : columns) {
				if (col.getName().equals(columnName)) {
					row.addValue(col, value);
					break;
				}
			}
		}
		rows.add(row);
	}

	public void insertRow(Row row) {
		rows.add(row);
	}

	public List<Row> getRows() {
		return rows;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		int[] columnWidths = new int[columns.size()];
		for (int i = 0; i < columns.size(); i++) {
			Column col = columns.get(i);
			columnWidths[i] = col.getName().length();

			for (Row row : rows) {
				Object value = row.getValue(col.getName());
				if (value != null) {
					columnWidths[i] = Math.max(columnWidths[i], value.toString().length());
				}
			}
		}

		for (int i = 0; i < columns.size(); i++) {
			String columnName = columns.get(i).getName();
			sb.append(String.format("%-" + columnWidths[i] + "s", columnName)).append("  ");
		}
		sb.append("\n");

		for (int width : columnWidths) {
			sb.append("-".repeat(width)).append("  ");
		}
		sb.append("\n");

		for (Row row : rows) {
			for (int i = 0; i < columns.size(); i++) {
				String columnName = columns.get(i).getName();
				Object value = row.getValue(columnName);
				sb.append(String.format("%-" + columnWidths[i] + "s", value != null ? value.toString() : ""))
						.append("  ");
			}
			sb.append("\n");
		}

		return sb.toString();
	}
}

class Column {
	String name;
	DataType type;

	public Column(String name, DataType type) {
		this.name = name;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public DataType getType() {
		return type;
	}
}

class Row {
	Map<String, Object> values;

	public Row() {
		values = new HashMap<>();
	}

	public void addValue(Column column, Object value) {
		values.put(column.getName(), value);
	}

	public Object getValue(String columnName) {
		return values.get(columnName);
	}

}

enum DataType {
	INTEGER, STRING, FLOAT, BOOLEAN, DECIMAL;
}