package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;

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
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * @author Rakshit
 * 
 *         The select operator is used to extract only those records that
 *         fulfill a specified criterion.
 *
 */
public class SelectOperator extends Eval implements Operator {

	private Expression where;
	private Operator parent;
	private Operator child;
	private TableInfo tableInfo;
	private ArrayList<String> tuple;
	private boolean output;

	/**
	 * This function checks if a particular tuple satisfies the select predicate
	 * and returns corresponding results.
	 * 
	 * @param
	 * @return ArrayList<String>
	 * @throws SQLException
	 */
	public ArrayList<String> getNext() throws SQLException {
		ArrayList<String> tempTuple = null;
		if (this.getChild() instanceof RelationOperator)
			tempTuple = ((RelationOperator) this.getChild()).getNext();
		else
			tempTuple = ((HashJoin) this.getChild()).getNext();

		/**
		 * Evaluates based on various possible select clauses.
		 */
		if (tempTuple != null) {
			this.setTuple(tempTuple);
			if (where != null) {
				where.accept(new ExpressionVisitor() {

					@Override
					public void visit(BitwiseXor arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(BitwiseOr arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(BitwiseAnd arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(Matches arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(Concat arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(AnyComparisonExpression arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(AllComparisonExpression arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(ExistsExpression arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(WhenClause arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(CaseExpression arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(SubSelect arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(Column arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(NotEqualsTo arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(MinorThanEquals arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(MinorThan arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(LikeExpression arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(IsNullExpression arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(InExpression arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(GreaterThanEquals arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(GreaterThan arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(EqualsTo arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(Between arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(OrExpression arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(AndExpression arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(Subtraction arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(Multiplication arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(Division arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(Addition arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}

					@Override
					public void visit(Parenthesis arg0) {
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
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
						try {
							if (eval(arg0).toString().equals("FALSE"))
								tuple.clear();
						} catch (SQLException e) {
							e.printStackTrace();
						}
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
			return tuple;
		}
		return null;
	}

	public Expression getWhere() {
		return where;
	}

	public void setWhere(Expression where) {
		this.where = where;
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

	public boolean isOutput() {
		return output;
	}

	public void setOutput(boolean output) {
		this.output = output;
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
}
