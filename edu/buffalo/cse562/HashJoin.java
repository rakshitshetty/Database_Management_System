package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.Join;

/**
 * @author Rakshit
 * 
 *         An SQL JOIN operator is used to combine rows from two or more tables,
 *         based on a common field between them. The hash join is a type of a
 *         join algorithm.
 */
public class HashJoin extends Eval implements Operator {

	private List<Join> relationList;
	private ArrayList<ArrayList<String>> cpTable = new ArrayList<ArrayList<String>>();
	private Stack<ArrayList<String>> joinTable = new Stack<ArrayList<String>>();
	private Operator child;
	private Operator parent;
	private TableInfo cpTableInfo;
	private ArrayList<String> tuple;
	private ArrayList<Expression> where;

	public ArrayList<Expression> getWhere() {
		return where;
	}

	public void setWhere(ArrayList<Expression> where) {
		this.where = where;
	}

	public List<Join> getRelationList() {
		return relationList;
	}

	public void setRelationList(List<Join> relationList) {
		this.relationList = relationList;
	}

	/**
	 * This function takes the tables to be joined and the condition as the
	 * parameter and returns a set of tuples which are the result of the join
	 * operation.
	 * 
	 * @param tables
	 * @param relationOperator
	 * @return
	 * @throws SQLException
	 */
	public TableInfo join(HashMap<String, TableInfo> tables,
			RelationOperator relationOperator) throws SQLException {

		ArrayList<String> tuple;
		String tname1, tname2;
		int t1 = 0, t2 = 0;
		HashMap<String, Integer> oo = new HashMap<String, Integer>();
		ArrayList<Integer> colnum = new ArrayList<Integer>();

		/*
		 * We retrieve the first table to be joined using the schema.
		 */
		String tableName = relationOperator.getRelation();
		tname1 = tableName;
		cpTableInfo = new TableInfo();
		cpTableInfo.getTable().setName("join");
		newSchema(cpTableInfo, tables.get(tableName));
		tables.put("join", cpTableInfo);

		while ((tuple = relationOperator.getNext()) != null) {
			cpTable.add(tuple);
		}

		/*
		 * We perform the hash join operation for subsequent tables in the list.
		 */
		ArrayList<Expression> expList = new ArrayList<Expression>();
		for (Join listItem : relationList) {
			tableName = listItem.getRightItem().toString();
			tname2 = tableName;
			String alias = listItem.getRightItem().getAlias();
			if (alias != null) {
				tableName = tableName.substring(0, tableName.indexOf(" AS"));
				tname2 = tableName;
				tables.get(tableName).getTable().setAlias(alias);
			}
			/*
			 * This determines the column that is to be used as the joining
			 * criteria and the corresponding column numnbers in the two tables.
			 */
			Expression on = listItem.getOnExpression();
			if (on == null) {
				for (Expression e : where) {
					if (e instanceof EqualsTo) {
						EqualsTo eq = (EqualsTo) e;
						Expression left = eq.getLeftExpression();
						Expression right = eq.getRightExpression();
						if ((left instanceof Column)
								&& (right instanceof Column)) {
							Column leftCol = (Column) left;
							Column rightCol = (Column) right;
							if ((tables.get(tname1).getTupleSchema()
									.containsKey(leftCol.getColumnName()) || tables
									.get(tname2).getTupleSchema()
									.containsKey(leftCol.getColumnName()))
									&& (tables
											.get(tname1)
											.getTupleSchema()
											.containsKey(
													rightCol.getColumnName()) || tables
											.get(tname2)
											.getTupleSchema()
											.containsKey(
													rightCol.getColumnName()))) {
								on = e;
								expList.add(e);
							}
						}
					} else if (e instanceof Parenthesis) {
						Parenthesis p = (Parenthesis) e;
						Expression exp = p.getExpression();
						if (exp instanceof EqualsTo) {
							EqualsTo eq = (EqualsTo) exp;
							Expression left = eq.getLeftExpression();
							Expression right = eq.getRightExpression();
							if ((left instanceof Column)
									&& (right instanceof Column)) {
								Column leftCol = (Column) left;
								Column rightCol = (Column) right;
								if ((tables.get(tname1).getTupleSchema()
										.containsKey(leftCol.getColumnName()) && tables
										.get(tname2).getTupleSchema()
										.containsKey(rightCol.getColumnName()))
										|| (tables
												.get(tname2)
												.getTupleSchema()
												.containsKey(
														leftCol.getColumnName()) && tables
												.get(tname1)
												.getTupleSchema()
												.containsKey(
														rightCol.getColumnName()))) {
									on = exp;
									expList.add(p);
								}
							}
						}
					}
				}
			}

			/*
			 * Creates a new schema for the joined table.
			 */
			relationOperator.setRelation(tableName);
			newSchema(cpTableInfo, tables.get(tableName));
			ArrayList<ArrayList<String>> tempTable = new ArrayList<ArrayList<String>>();

			while ((tuple = relationOperator.getNext()) != null) {
				tempTable.add(tuple);
			}

			/*
			 * Resolving naming conventions.
			 */
			if (on != null) {
				String condition = on.toString();

				String c[] = condition.split(" = ");

				String l[] = c[0].split("[.]");
				if (l[0].equalsIgnoreCase(tname1)) {
					HashMap<String, Integer> tup1 = tables.get(tname1)
							.getTupleSchema();
					t1 = tup1.get(l[1]);
				}

				else if (l[0].equalsIgnoreCase(tname2)) {
					HashMap<String, Integer> tup2 = tables.get(tname2)
							.getTupleSchema();
					t2 = tup2.get(l[1]);
				}

				String r[] = c[1].split("[.]");
				if (r[0].equalsIgnoreCase(tname1)) {
					HashMap<String, Integer> tup1 = tables.get(tname1)
							.getTupleSchema();
					t1 = tup1.get(l[1]);
				}

				else if (r[0].equalsIgnoreCase(tname2)) {
					HashMap<String, Integer> tup2 = tables.get(tname2)
							.getTupleSchema();
					t2 = tup2.get(l[1]);
				}
			}

			/*
			 * Hashes and joins the table.
			 */
			cpTable = hash(cpTable, tempTable, on, t1, t2);
			tname1 = "join";
		}

		where.removeAll(expList);
		joinTable.addAll(cpTable);
		return cpTableInfo;
	}

	/**
	 * This function hashes a particular tables and then joins the second table
	 * based on the hash created. It returns the resultant joined table.
	 * 
	 * @param table1
	 * @param table2
	 * @param on
	 * @param t1
	 * @param t2
	 * @return
	 */
	public ArrayList<ArrayList<String>> hash(
			ArrayList<ArrayList<String>> table1,
			ArrayList<ArrayList<String>> table2, Expression on, int t1, int t2) {
		ArrayList<ArrayList<String>> table3 = new ArrayList<ArrayList<String>>();

		HashMap<String, LinkedList<ArrayList<String>>> hashbuck = new HashMap<String, LinkedList<ArrayList<String>>>();
		/*
		 * Hashes the first table and then hashes the second table to join.
		 */
		if (on != null) {
			for (ArrayList<String> tuple : table1) {
				if (!hashbuck.containsKey(tuple.get(t1))) {
					LinkedList<ArrayList<String>> list = new LinkedList<ArrayList<String>>();
					list.add(tuple);
					hashbuck.put(tuple.get(t1), list);
				} else {
					LinkedList<ArrayList<String>> list = hashbuck.get(tuple
							.get(t1));
					list.add(tuple);
					hashbuck.put(tuple.get(t1), list);
				}
			}

			for (ArrayList<String> tuple2 : table2) {
				if (hashbuck.containsKey(tuple2.get(t2))) {
					LinkedList<ArrayList<String>> list = hashbuck.get(tuple2
							.get(t2));
					for (ArrayList<String> q : list) {
						ArrayList<String> temp = new ArrayList<String>();

						temp.addAll(q);
						temp.addAll(tuple2);
						if (temp.size() > 0)
							table3.add(temp);
					}
				}
			}
		} else {
			for (ArrayList<String> tuple1 : table1) {
				for (ArrayList<String> tuple2 : table2) {
					ArrayList<String> temp = new ArrayList<String>();

					temp.addAll(tuple1);
					temp.addAll(tuple2);
					table3.add(temp);
				}
			}
		}

		return table3;
	}

	/**
	 * Creates a new schema for the joined table.
	 * 
	 * @param newTableInfo
	 * @param oldTableInfo
	 * @return
	 */
	public TableInfo newSchema(TableInfo newTableInfo, TableInfo oldTableInfo) {

		String t = oldTableInfo.getTable().getName().toLowerCase();
		if (oldTableInfo.getTable().getAlias() != null)
			t = oldTableInfo.getTable().getAlias();
		int size = newTableInfo.getTupleSchema().size();
		for (Map.Entry<String, Integer> entry : oldTableInfo.getTupleSchema()
				.entrySet()) {
			String newColName = t + "." + entry.getKey();
			Integer newColID = size + entry.getValue();
			newTableInfo.getOldTupleSchema().put(entry.getKey(), newColID);
			newTableInfo.getTupleSchema().put(newColName, newColID);

		}

		size = newTableInfo.getSchema().size();
		for (Map.Entry<String, ColumnDefinition> entry : oldTableInfo
				.getSchema().entrySet()) {
			String newColName = t + "." + entry.getKey();
			ColumnDefinition newCD = entry.getValue();
			newCD.setColumnName(newColName);
			newTableInfo.getOldSchema().put(entry.getKey(), newCD);
			newTableInfo.getSchema().put(newColName, newCD);

		}
		return newTableInfo;
	}

	public ArrayList<String> getNext() throws SQLException {
		if (!(joinTable.empty()))
			return joinTable.pop();
		return null;
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

	/**
	 * This function evaluates the column based on the type of data present in
	 * it.
	 * 
	 * @param Column
	 *            arg0
	 * @return
	 * @throws SQLException
	 */
	@Override
	public LeafValue eval(Column arg0) throws SQLException {
		Integer colID = cpTableInfo.getTupleSchema().get(
				arg0.getWholeColumnName());
		String dataType;
		if (colID == null) {
			colID = this.cpTableInfo.getOldTupleSchema().get(
					arg0.getColumnName());
			dataType = this.cpTableInfo.getOldSchema()
					.get(arg0.getColumnName()).getColDataType().getDataType()
					.toLowerCase();
		} else
			dataType = this.cpTableInfo.getSchema()
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

}
