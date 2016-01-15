package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.sun.org.apache.xpath.internal.axes.HasPositionalPredChecker;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

/**
 * @author Rakshit
 * 
 *         The PROJECT operator is used to select a subset of the attributes of
 *         a relation by specifying the names of the required attributes.
 * 
 */
public class ProjectOperator extends Eval implements Operator {

	private List<SelectItem> selectItems;
	private Operator parent;
	private Operator child;
	private TableInfo tableInfo;
	private ArrayList<String> tuple;
	private List<Column> groupBy;
	private LinkedHashMap<String, HashMap<ArrayList<String>, ArrayList<LeafValue>>> group1 = new LinkedHashMap<String, HashMap<ArrayList<String>, ArrayList<LeafValue>>>();
	private boolean flag = true;

	/**
	 * This function returns a tuple after projecting the required attributes.
	 * It also resolves aggregate functions such as MAX, MIN etc.
	 * 
	 * @return ArrayList<String>
	 * @throws SQLException
	 */
	public ArrayList<String> getNext() throws SQLException {

		ArrayList<String> tempTuple = this.getChild().getNext();
		ArrayList<String> tuple = new ArrayList<String>();
		if ((tempTuple != null && tempTuple.size() != 0) || flag) {
			this.setTuple(tempTuple);

			/*
			 * Iterates through each attribute present in the project clause.
			 */
			for (SelectItem item : selectItems) {
				item.accept(new SelectItemVisitor() {
					@Override
					public void visit(SelectExpressionItem arg0) {
						try {
							if (arg0.getExpression() instanceof Function) {
								Function fn = (Function) arg0.getExpression();
								String function = fn.toString();
								String fnName = fn.getName();
								if (!group1.containsKey(function))
									group1.put(
											function,
											new HashMap<ArrayList<String>, ArrayList<LeafValue>>());
								ExpressionList parameters = fn.getParameters();
								if (parameters == null) {
									if (tempTuple == null) {
										if (flag) {
											if (groupBy == null) {
												for (HashMap<ArrayList<String>, ArrayList<LeafValue>> entry1 : group1
														.values()) {
													for (ArrayList<LeafValue> entry : entry1
															.values()) {
														tuple.add(entry.get(0)
																.toString());
													}
												}
											} else {
												boolean first = true;
												ArrayList<String> key = new ArrayList<String>();
												for (HashMap<ArrayList<String>, ArrayList<LeafValue>> entry1 : group1
														.values()) {
													for (Entry<ArrayList<String>, ArrayList<LeafValue>> entry : entry1
															.entrySet()) {
														if (first) {
															key = entry
																	.getKey();
															tuple.addAll(key);
															first = false;
														}
														if (entry.getKey() == key)
															tuple.add(entry
																	.getValue()
																	.get(0)
																	.toString());
													}
													first = true;
													tuple.add("^!$");
												}
											}
										}
										flag = false;
									} else if (tempTuple.size() != 0) {
										if (function.contains("count(")) {
											if (groupBy == null) {
												count(function, null);
											} else {
												ArrayList<String> groupByItems = new ArrayList<String>();
												for (Column c : groupBy) {
													groupByItems.add(eval(c)
															.toString());
												}
												count(function, groupByItems);
											}
										}
									}
								}
								/*
								 * Resolves aggregate functions such as count,
								 * max, min etc.
								 */
								else {
									Expression e = (Expression) parameters
											.getExpressions().get(0);
									if (tempTuple == null) {
										if (flag) {
											if (groupBy == null) {
												for (Entry<String, HashMap<ArrayList<String>, ArrayList<LeafValue>>> entry1 : group1
														.entrySet()) {
													HashMap<ArrayList<String>, ArrayList<LeafValue>> group = entry1
															.getValue();
													if (entry1.getKey()
															.contains("count(")
															&& !entry1
																	.getKey()
																	.contains(
																			"*")) {
														for (ArrayList<LeafValue> entry : group
																.values()) {
															tuple.add(entry
																	.size()
																	+ "");
														}
													} else if (entry1.getKey()
															.contains("avg")) {
														for (ArrayList<LeafValue> entry : group
																.values()) {
															tuple.add(entry
																	.get(2)
																	.toString());
														}
													} else {
														for (ArrayList<LeafValue> entry : group
																.values()) {
															tuple.add(entry
																	.get(0)
																	.toString());
														}
													}
												}
											} else {
												Set<ArrayList<String>> keySet = null;
												for (Entry<String, HashMap<ArrayList<String>, ArrayList<LeafValue>>> entry1 : group1
														.entrySet()) {
													HashMap<ArrayList<String>, ArrayList<LeafValue>> group = entry1
															.getValue();
													keySet = group.keySet();
													break;
												}
												for (ArrayList<String> key : keySet) {
													tuple.addAll(key);
													for (Entry<String, HashMap<ArrayList<String>, ArrayList<LeafValue>>> entry1 : group1
															.entrySet()) {
														HashMap<ArrayList<String>, ArrayList<LeafValue>> group = entry1
																.getValue();

														if (entry1
																.getKey()
																.contains("avg")) {
															tuple.add(group
																	.get(key)
																	.get(2)
																	.toString());
														} else
															tuple.add(group
																	.get(key)
																	.get(0)
																	.toString());
													}

													tuple.add("^!$");
												}

											}
										}
										flag = false;
									} else if (tempTuple.size() != 0) {
										ArrayList<String> cValue = new ArrayList<String>();
										if (groupBy == null) {
											cValue.add(e.toString());
											HashMap<ArrayList<String>, ArrayList<LeafValue>> group = group1
													.get(function);
											if (group.containsKey(cValue)) {
												ArrayList<LeafValue> l = group
														.get(cValue);
												l.add(eval(e));
												group.put(cValue, l);
											} else {
												ArrayList<LeafValue> l = new ArrayList<LeafValue>();
												l.add(eval(e));
												group.put(cValue, l);
											}
										} else {
											for (Column c : groupBy) {
												cValue.add(eval(c).toString());
											}
											HashMap<ArrayList<String>, ArrayList<LeafValue>> group = group1
													.get(function);
											if (group.containsKey(cValue)) {
												ArrayList<LeafValue> l = group
														.get(cValue);
												l.add(eval(e));
												group.put(cValue, l);
											} else {
												ArrayList<LeafValue> l = new ArrayList<LeafValue>();
												l.add(eval(e));
												group.put(cValue, l);
											}
										}

										if (fnName.equalsIgnoreCase("SUM")) {
											sum(function, cValue);
										} else if (fnName
												.equalsIgnoreCase("AVG")) {
											avg(function, cValue);
										} else if (fnName
												.equalsIgnoreCase("MIN")) {
											min(function, cValue);
										} else if (fnName
												.equalsIgnoreCase("MAX")) {
											max(function, cValue);
										}
									}
								}
							} else if (!(arg0.getExpression() instanceof Function)
									&& groupBy == null) {
								flag = false;
								if (tempTuple != null && tempTuple.size() != 0)
									tuple.add(eval(arg0.getExpression())
											.toString());
							}

						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(AllTableColumns arg0) {

					}

					@Override
					public void visit(AllColumns arg0) {
						flag = false;
						if (tempTuple != null && tempTuple.size() != 0)
							tuple.addAll(tempTuple);
					}
				});
			}
			// System.out.println(group1);
			// System.out.println(tuple);
			return tuple;
		}

		if (tempTuple == null)
			return null;
		return tuple;
	}

	/**
	 * This function evaluates the column based on the type of data present in
	 * it.
	 * 
	 * @param Column  arg0
	 * @return
	 * @throws SQLException
	 */
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		Integer colID = this.tableInfo.getTupleSchema().get(
				arg0.getWholeColumnName());
		String dataType;
		if (colID == null) {
			colID = this.tableInfo.getOldTupleSchema()
					.get(arg0.getColumnName());
			dataType = this.tableInfo.getOldSchema().get(arg0.getColumnName())
					.getColDataType().getDataType().toLowerCase();
		} else
			dataType = this.tableInfo.getSchema()
					.get(arg0.getWholeColumnName()).getColDataType()
					.getDataType().toLowerCase();

		LeafValue value = null;
		if (dataType.contains("int"))
			value = new LongValue(tuple.get(colID));
		else if (dataType.contains("decimal"))
			value = new DoubleValue(tuple.get(colID));
		else if (dataType.contains("varchar") || dataType.contains("char")
				|| dataType.contains("string"))
			value = new StringValue("'" + tuple.get(colID) + "'");
		else if (dataType.contains("date"))
			value = new DateValue(" " + tuple.get(colID) + " ");

		return value;
	}

	/**
	 * This function takes a column as the parameter and computes the sum of
	 * this column.
	 * 
	 * @param fnName
	 * @param columnName
	 */
	public void sum(String fnName, ArrayList<String> columnName) {
		try {
			HashMap<ArrayList<String>, ArrayList<LeafValue>> group = group1
					.get(fnName);
			ArrayList<LeafValue> l = group.get(columnName);
			LeafValue sum = new DoubleValue(0.0);
			for (LeafValue lv : l) {
				sum = new DoubleValue(lv.toDouble() + sum.toDouble());
			}
			l.clear();
			l.add(sum);
			group.put(columnName, l);
		} catch (InvalidLeaf e) {
			e.printStackTrace();
		}
	}

	/**
	 * This function takes the column as the parameter and computes the average
	 * on that column.
	 * 
	 * @param fnName
	 * @param columnName
	 */
	public void avg(String fnName, ArrayList<String> columnName) {
		try {
			HashMap<ArrayList<String>, ArrayList<LeafValue>> group = group1
					.get(fnName);
			ArrayList<LeafValue> l = group.get(columnName);
			long count = 0;
			if (l.size() > 1)
				count = l.get(1).toLong();
			LeafValue sum = new DoubleValue(l.get(0).toDouble());
			if (l.size() == 4) {
				sum = new DoubleValue(l.get(3).toDouble() + sum.toDouble());
			}
			count++;
			// System.out.println(count);
			LeafValue avg = new DoubleValue(sum.toDouble() / count);
			l.clear();
			l.add(sum);
			l.add(new LongValue(count));
			l.add(avg);
			group.put(columnName, l);
		} catch (InvalidLeaf e) {
			e.printStackTrace();
		}
	}

	/**
	 * This function takes a column as the parameter and computes the maximum
	 * values of that column.
	 * 
	 * @param fnName
	 * @param columnName
	 */
	public void max(String fnName, ArrayList<String> columnName) {
		try {
			HashMap<ArrayList<String>, ArrayList<LeafValue>> group = group1
					.get(fnName);
			ArrayList<LeafValue> l = group.get(columnName);
			LeafValue max;
			if (l.get(0) instanceof LongValue) {
				max = new LongValue(0);
				for (LeafValue lv : l) {
					if (lv.toLong() > max.toLong())
						max = lv;
				}
			} else {
				max = new DoubleValue(0.0);
				for (LeafValue lv : l) {
					if (lv.toDouble() < max.toDouble())
						max = lv;
				}
			}
			l.clear();
			l.add(max);
			group.put(columnName, l);
		} catch (InvalidLeaf e) {
			e.printStackTrace();
		}
	}

	/**
	 * This function takes the column as the parameter and computes the minimum
	 * value in that column.
	 * 
	 * @param fnName
	 * @param columnName
	 */
	public void min(String fnName, ArrayList<String> columnName) {
		try {
			HashMap<ArrayList<String>, ArrayList<LeafValue>> group = group1
					.get(fnName);
			ArrayList<LeafValue> l = group.get(columnName);
			LeafValue min;
			if (l.get(0) instanceof LongValue) {
				min = new LongValue(l.get(0).toLong());
				for (LeafValue lv : l) {
					if (lv.toLong() < min.toLong())
						min = lv;
				}
			} else {
				min = new DoubleValue(l.get(0).toDouble());
				for (LeafValue lv : l) {
					if (lv.toDouble() < min.toDouble())
						min = lv;
				}
			}
			l.clear();
			l.add(min);
			group.put(columnName, l);
		} catch (InvalidLeaf e) {
			e.printStackTrace();
		}
	}

	/**
	 * This function takes a column as the parameter and computes the count of
	 * each group in that column.
	 * 
	 * @param fnName
	 * @param columnName
	 */
	public void count(String fnName, ArrayList<String> columnName) {
		HashMap<ArrayList<String>, ArrayList<LeafValue>> group = group1
				.get(fnName);
		if (columnName == null) {
			columnName = new ArrayList<String>();
			columnName.add("*");
		}
		try {
			if (group.containsKey(columnName)) {
				ArrayList<LeafValue> lv = group.get(columnName);
				LongValue l = new LongValue(lv.get(0).toLong() + 1);
				lv.clear();
				lv.add(l);
				group.put(columnName, lv);
			} else {
				ArrayList<LeafValue> lv = new ArrayList<LeafValue>();
				LongValue l = new LongValue(1);
				lv.add(l);
				group.put(columnName, lv);
			}
		} catch (InvalidLeaf e) {
			e.printStackTrace();
		}
	}

	public List<SelectItem> getSelectItems() {
		return selectItems;
	}

	public void setSelectItems(List<SelectItem> selectItems) {
		this.selectItems = selectItems;
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

	public ArrayList<String> getTuple() {
		return tuple;
	}

	public void setTuple(ArrayList<String> tuple) {
		this.tuple = tuple;
	}

	public List<Column> getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(List<Column> groupBy) {
		this.groupBy = groupBy;
	}

}
