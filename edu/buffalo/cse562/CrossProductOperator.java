package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.TableInfo;

/**
 * @author Rakshit
 * 
 *         CrossProduct operator returns the Cartesian product of the sets of
 *         records from the two or more joined tables.
 *
 */
public class CrossProductOperator extends Eval implements Operator {
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
	 * This function takes the list of tables as the parameter and returns the
	 * schema of the resultant relation and loads the result into joinTable
	 * class member.
	 * 
	 * @param tables
	 * @param relationOperator
	 * @return
	 * @throws SQLException
	 */
	public TableInfo join(HashMap<String, TableInfo> tables,
			RelationOperator relationOperator) throws SQLException {
		ArrayList<String> tuple;
		String tableName = relationOperator.getRelation();
		cpTableInfo = new TableInfo();
		cpTableInfo.getTable().setName("cross product");
		newSchema(cpTableInfo, tables.get(tableName));

		while ((tuple = relationOperator.getNext()) != null) {
			cpTable.add(tuple);
		}

		/**
		 * Performs cross product for every relation present in the list.
		 */
		for (Join listItem : relationList) {
			tableName = listItem.getRightItem().toString();
			String alias = listItem.getRightItem().getAlias();
			if (alias != null) {
				tableName = tableName.substring(0, tableName.indexOf(" AS"));
				tables.get(tableName).getTable().setAlias(alias);
			}
			Expression on = listItem.getOnExpression();

			relationOperator.setRelation(tableName);
			newSchema(cpTableInfo, tables.get(tableName));
			ArrayList<ArrayList<String>> tempTable = new ArrayList<ArrayList<String>>();
			while ((tuple = relationOperator.getNext()) != null) {
				tempTable.add(tuple);
			}
			cpTable = crossProduct(cpTable, tempTable, on);
		}

		// System.out.println(cpTable);
		joinTable.addAll(cpTable);
		return cpTableInfo;
	}

	/**
	 * This function takes two tables and an expression as the parameter and
	 * returns the joined table. The expression in the parameter is the criteria
	 * on which the cross product is performed.
	 * 
	 * @param table1
	 * @param table2
	 * @param on
	 * @return
	 */
	public ArrayList<ArrayList<String>> crossProduct(
			ArrayList<ArrayList<String>> table1,
			ArrayList<ArrayList<String>> table2, Expression on) {
		ArrayList<ArrayList<String>> table3 = new ArrayList<ArrayList<String>>();
		for (ArrayList<String> tuple1 : table1) {
			for (ArrayList<String> tuple2 : table2) {
				ArrayList<String> temp = new ArrayList<String>();
				temp.addAll(tuple1);
				temp.addAll(tuple2);
				tuple = temp;

				/*
				 * Resolves the criteria
				 */
				if (on != null) {
					on.accept(new ExpressionVisitor() {

						@Override
						public void visit(EqualsTo arg0) {
							try {
								if (eval(arg0).toString().equals("FALSE"))
									temp.clear();
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}

						@Override
						public void visit(Parenthesis arg0) {
							try {
								if (eval(arg0).toString().equals("FALSE"))
									temp.clear();
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}

						@Override
						public void visit(BitwiseXor arg0) {

						}

						@Override
						public void visit(BitwiseOr arg0) {

						}

						@Override
						public void visit(BitwiseAnd arg0) {

						}

						@Override
						public void visit(Matches arg0) {

						}

						@Override
						public void visit(Concat arg0) {

						}

						@Override
						public void visit(AnyComparisonExpression arg0) {

						}

						@Override
						public void visit(AllComparisonExpression arg0) {

						}

						@Override
						public void visit(ExistsExpression arg0) {

						}

						@Override
						public void visit(WhenClause arg0) {

						}

						@Override
						public void visit(CaseExpression arg0) {

						}

						@Override
						public void visit(SubSelect arg0) {

						}

						@Override
						public void visit(Column arg0) {

						}

						@Override
						public void visit(NotEqualsTo arg0) {
							try {
								if (eval(arg0).toString().equals("FALSE"))
									temp.clear();
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}

						@Override
						public void visit(MinorThanEquals arg0) {
							try {
								if (eval(arg0).toString().equals("FALSE"))
									temp.clear();
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}

						@Override
						public void visit(MinorThan arg0) {
							try {
								if (eval(arg0).toString().equals("FALSE"))
									temp.clear();
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}

						@Override
						public void visit(LikeExpression arg0) {

						}

						@Override
						public void visit(IsNullExpression arg0) {

						}

						@Override
						public void visit(InExpression arg0) {

						}

						@Override
						public void visit(GreaterThanEquals arg0) {
							try {
								if (eval(arg0).toString().equals("FALSE"))
									temp.clear();
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}

						@Override
						public void visit(GreaterThan arg0) {
							try {
								if (eval(arg0).toString().equals("FALSE"))
									temp.clear();
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}

						@Override
						public void visit(Between arg0) {

						}

						@Override
						public void visit(OrExpression arg0) {

						}

						@Override
						public void visit(AndExpression arg0) {

						}

						@Override
						public void visit(Subtraction arg0) {

						}

						@Override
						public void visit(Multiplication arg0) {

						}

						@Override
						public void visit(Division arg0) {

						}

						@Override
						public void visit(Addition arg0) {

						}

						@Override
						public void visit(StringValue arg0) {

						}

						@Override
						public void visit(TimestampValue arg0) {

						}

						@Override
						public void visit(TimeValue arg0) {

						}

						@Override
						public void visit(DateValue arg0) {

						}

						@Override
						public void visit(LongValue arg0) {

						}

						@Override
						public void visit(DoubleValue arg0) {

						}

						@Override
						public void visit(JdbcParameter arg0) {

						}

						@Override
						public void visit(InverseExpression arg0) {

						}

						@Override
						public void visit(Function arg0) {

						}

						@Override
						public void visit(NullValue arg0) {

						}
					});
				}
				if (temp.size() > 0)
					table3.add(temp);
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
		String t = oldTableInfo.getTable().getName();
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
	 * @param Column arg0
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
