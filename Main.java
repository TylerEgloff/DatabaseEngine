import java.util.*;
import java.math.*;

public class Main {
	public static void main(String[] args) throws Exception {
		Operations ra = new Operations();

		Table usersTable = new Table("users");
		Table productsTable = new Table("products");
		Table ordersTable = new Table("orders");
		Table orderItemsTable = new Table("order_items");

		usersTable.addColumn(new Column("user_id", DataType.INTEGER));
		usersTable.addColumn(new Column("first_name", DataType.STRING));
		usersTable.addColumn(new Column("last_name", DataType.STRING));
		usersTable.addColumn(new Column("address", DataType.STRING));
		usersTable.addColumn(new Column("email", DataType.STRING));

		productsTable.addColumn(new Column("product_id", DataType.INTEGER));
		productsTable.addColumn(new Column("product_name", DataType.STRING));
		productsTable.addColumn(new Column("description", DataType.STRING));
		productsTable.addColumn(new Column("price", DataType.DECIMAL));

		ordersTable.addColumn(new Column("order_id", DataType.INTEGER));
		ordersTable.addColumn(new Column("user_id", DataType.INTEGER));
		ordersTable.addColumn(new Column("order_date", DataType.STRING));

		orderItemsTable.addColumn(new Column("order_id", DataType.INTEGER));
		orderItemsTable.addColumn(new Column("product_id", DataType.INTEGER));
		orderItemsTable.addColumn(new Column("quantity", DataType.INTEGER)); 
		orderItemsTable.addColumn(new Column("price", DataType.DECIMAL));

		usersTable.insert(Arrays.asList("user_id", "first_name", "last_name", "address", "email"),
				Arrays.asList(1, "Rudolf", "Bayer", "123 Main St", "rb@gmail.com"));
		usersTable.insert(Arrays.asList("user_id", "first_name", "last_name", "address", "email"),
				Arrays.asList(2, "Edward", "McCreight", "456 Main St", "ec@gmail.com"));

		productsTable.insert(Arrays.asList("product_id", "product_name", "description", "price"),
				Arrays.asList(1, "Laptop", "A lenovo", new BigDecimal("1200.00")));
		productsTable.insert(Arrays.asList("product_id", "product_name", "description", "price"),
				Arrays.asList(2, "Smartphone", "An iPhone", new BigDecimal("800.00")));

		ordersTable.insert(Arrays.asList("order_id", "user_id", "order_date"), Arrays.asList(1, 1, "2024-10-01"));
		ordersTable.insert(Arrays.asList("order_id", "user_id", "order_date"), Arrays.asList(2, 2, "2024-10-02"));

		orderItemsTable.insert(Arrays.asList("order_id", "product_id", "quantity", "price"),
				Arrays.asList(1, 1, 1, new BigDecimal("1200.00")));
		orderItemsTable.insert(Arrays.asList("order_id", "product_id", "quantity", "price"),
				Arrays.asList(1, 2, 1, new BigDecimal("800.00")));
		orderItemsTable.insert(Arrays.asList("order_id", "product_id", "quantity", "price"),
				Arrays.asList(2, 2, 1, new BigDecimal("800.00")));

		// This query returns a name and order date for recent transactions
		Table joinedTable = ra.naturalJoin(ordersTable, usersTable);
		
		// row -> true is equivalent to select all
		// try this condition: row -> row.getValue("order_date").equals("2024-10-01");
		Table selectionTable = ra.select(joinedTable, row -> true);

		// Pass a list of columns
		Table projectionTable = ra.project(selectionTable, Arrays.asList("first_name", "last_name", "order_date"));

		System.out.println(projectionTable);

	}
}
