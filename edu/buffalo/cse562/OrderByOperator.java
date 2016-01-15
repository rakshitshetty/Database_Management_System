package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;

/**
 * @author Rakshit
 * 
 *         The ORDER BY operator is used to sort the result-set by one or more
 *         columns. The ORDER BY keyword sorts the records in ascending order or
 *         descending order.
 *
 */
public class OrderByOperator implements Operator {

	private Operator parent;
	private Operator child;
	private TableInfo tableInfo;
	private List<OrderByElement> orderBy;
	private ArrayList<String> tuple;

	/**
	 * This function takes a set of tuples as the input and returns an ordered
	 * set of tuples according to the order specified.
	 * 
	 * @param ArrayList
	 *            <ArrayList<String>>
	 * @return ArrayList<ArrayList<String>>
	 */
	public ArrayList<ArrayList<String>> order(ArrayList<ArrayList<String>> o) {
		Stack<OrderByElement> stack = new Stack<OrderByElement>();
		for (OrderByElement element : orderBy) {
			stack.push(element);
		}

		/*
		 * Uses a stack to keep track of all the elements to be oredered by.
		 */
		while (!stack.empty()) {
			OrderByElement element = stack.pop();
			element.accept(new OrderByVisitor() {

				@Override
				public void visit(OrderByElement arg0) {
					int colID = tableInfo.getTupleSchema().get(
							arg0.getExpression().toString());
					boolean isAsc = arg0.isAsc();
					String dataType = tableInfo.getSchema()
							.get(arg0.getExpression().toString())
							.getColDataType().getDataType().toLowerCase();
					/*
					 * Sorts the set of tuples based on a custom comparator.
					 */
					Collections.sort(o, new Comparator<ArrayList<String>>() {
						@Override
						public int compare(ArrayList<String> one,
								ArrayList<String> two) {
							if (dataType.contains("int")) {
								Long value1 = new Long(one.get(colID));
								Long value2 = new Long(two.get(colID));
								if (isAsc)
									return value1.compareTo(value2);
								else
									return value2.compareTo(value1);
							} else if (dataType.contains("decimal")) {
								Double value1 = new Double(one.get(colID));
								Double value2 = new Double(two.get(colID));
								if (isAsc)
									return value1.compareTo(value2);
								else
									return value2.compareTo(value1);
							} else if (dataType.contains("varchar")
									|| dataType.contains("char")
									|| dataType.contains("string")) {
								if (isAsc)
									return one.get(colID).compareTo(
											two.get(colID));
								else
									return two.get(colID).compareTo(
											one.get(colID));
							} else if (dataType.contains("date")) {
								DateValue dv1 = new DateValue(" "
										+ one.get(colID) + " ");
								DateValue dv2 = new DateValue(" "
										+ two.get(colID) + " ");
								if (isAsc)
									return dv1.getValue().compareTo(
											dv2.getValue());
								else
									return dv2.getValue().compareTo(
											dv1.getValue());
							}

							return 0;
						}
					});
				}
			});
		}

		return o;
	}

	public Operator getParent() {
		return parent;
	}

	public void setParent(Operator parent) {
		this.parent = parent;
	}

	public Operator getChild() {
		return child;
	}

	public void setChild(Operator child) {
		this.child = child;
	}

	public TableInfo getTableInfo() {
		return tableInfo;
	}

	public void setTableInfo(TableInfo tableInfo) {
		this.tableInfo = tableInfo;
	}

	@Override
	public ArrayList<String> getNext() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<OrderByElement> getOrderBy() {
		return orderBy;
	}

	public void setOrderBy(List<OrderByElement> orderBy) {
		this.orderBy = orderBy;
	}

	public ArrayList<String> getTuple() {
		return tuple;
	}

	public void setTuple(ArrayList<String> tuple) {
		this.tuple = tuple;
	}

}
