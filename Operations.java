
import java.util.*;

public class Operations {

	@FunctionalInterface
	interface RowCondition {
		boolean matches(Row row);
	}

	public Table select(Table table, RowCondition condition) {
		Table tempTable = new Table(table.tableName + "_select");
		tempTable.columns.addAll(table.columns); // Add columns

		// Filter rows
		for (Row row : table.rows) {
			if (condition.matches(row)) {
				tempTable.insertRow(row);
			}
		}

		return tempTable;
	}

	public Table project(Table table, List<String> columnNames) {
		Table tempTable = new Table(table.tableName + "_project");

		for (String colName : columnNames) {
			for (Column col : table.columns) {
				if (col.getName().equals(colName)) {
					tempTable.addColumn(col);
				}
			}
		}

		// Add rows from selected columns
		for (Row row : table.rows) {
			Row newRow = new Row();
			for (String colName : columnNames) {
				Column selectedColumn = table.columns.stream().filter(col -> col.getName().equals(colName)).findFirst()
						.orElse(null);

				if (selectedColumn != null) {
					newRow.addValue(new Column(colName, selectedColumn.getType()), row.getValue(colName));
				}
			}
			tempTable.insertRow(newRow);
		}

		return tempTable;
	}

	public Table naturalJoin(Table table1, Table table2) {
	    Table tempTable = new Table(table1.tableName + "_natural_join_" + table2.tableName);

	    // Constructs a set of common col names and adds all columns from table1 to a temp table
	    Set<String> commonColumnNames = new HashSet<>();
	    for (Column col1 : table1.columns) {
	        for (Column col2 : table2.columns) {
	            if (col1.getName().equals(col2.getName()) && col1.getType() == col2.getType()) {
	                commonColumnNames.add(col1.getName());
	            }
	        }
	        tempTable.columns.add(col1);
	    }

	    // Any cols not found in table1 get added to the temporary table
	    for (Column col2 : table2.columns) {
	        if (!commonColumnNames.contains(col2.getName())) {
	            tempTable.columns.add(col2);
	        }
	    }

	    // Construct hashmap for table2 data
	    Map<String, List<Row>> hashMap = new HashMap<>();
	    for (Row row2 : table2.rows) {
	        StringBuilder key = new StringBuilder();
	        for (String colName : commonColumnNames) {
	            key.append(row2.getValue(colName).toString()).append("|");
	        }
	        hashMap.computeIfAbsent(key.toString(), k -> new ArrayList<>()).add(row2);
	    }

	    // Join rows from table1 and table2
	    for (Row row1 : table1.rows) {
	        StringBuilder key = new StringBuilder();
	        for (String colName : commonColumnNames) {
	            key.append(row1.getValue(colName).toString()).append("|");
	        }

	        List<Row> matchingRows = hashMap.get(key.toString());
	        if (matchingRows != null) {
	            for (Row row2 : matchingRows) {
	                Row joinedRow = new Row();
	                joinedRow.values.putAll(row1.values);  // First all row1 values

	                // Next, all not already present columns
	                for (Column col2 : table2.columns) {
	                    if (!commonColumnNames.contains(col2.getName())) {
	                        joinedRow.addValue(col2, row2.getValue(col2.getName()));
	                    }
	                }
	                tempTable.insertRow(joinedRow);
	            }
	        }
	    }

	    return tempTable;
	}


	public Table distinct(Table table, List<String> projectionColumns) {
		Set<List<Object>> seenRows = new HashSet<>();
		Table tempTable = new Table(table.tableName + "_distinct");
		tempTable.columns.addAll(table.columns);

		// Check if row has been seen before
		for (Row row : table.rows) {
			List<Object> key = new ArrayList<>();
			for (String colName : projectionColumns) {
				key.add(row.getValue(colName));
			}

			if (!seenRows.contains(key)) {
				seenRows.add(key);
				tempTable.insertRow(row);
			}
		}

		return tempTable;
	}

	public void alterTable(Table table, Column column) {
		boolean columnExists = false;

		// Find matching index and replace it
		for (int i = 0; i < table.columns.size(); i++) {
			if (table.columns.get(i).getName().equals(column.getName())) {
				table.columns.set(i, column);
				columnExists = true;
				break;
			}
		}

		// If it's not a column, add it
		if (!columnExists) {
			table.addColumn(column);
		}
	}

	public void removeColumn(Table table, String columnName) {
		table.columns.removeIf(column -> column.getName().equals(columnName));
	}

	public void removeRow(Table table, RowCondition condition) {
		table.rows.removeIf(row -> condition.matches(row));
	}

}
