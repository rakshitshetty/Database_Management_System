package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

/**
 * @author Rakshit
 * 
 * The GROUP BY operator is used in conjunction with the 
 * aggregate functions to group the result-set by one or more columns.
 *
 */
public class GroupByOperator extends Eval implements Operator {

	private TableInfo tableInfo;
	private ArrayList<String> tuple;
	private Operator parent;
	private Operator child;
	private List<SelectItem> selectItems;
	private List<Column> groupBy;

	/**
	 * This function takes in a set of tuples and returns 
	 * the set of tuples grouped by one or more specified
	 * columns.
	 * @param ArrayList<ArrayList<String>>
	 * @return
	 */
	public ArrayList<ArrayList<String>> output(ArrayList<ArrayList<String>> o) {
		HashMap<String, Integer> tupleSchema = new HashMap<String, Integer>();
		HashMap<String, ColumnDefinition> schema = new HashMap<String, ColumnDefinition>();
		int count = 0;
		for (Column c : groupBy) {
			tupleSchema.put(c.getWholeColumnName(), count++);
			ColumnDefinition cd = this.tableInfo.getOldSchema().get(
					c.getColumnName());
			if (cd == null)
				cd = this.tableInfo.getSchema().get(c.getWholeColumnName());
			schema.put(c.getWholeColumnName(), cd);
		}


		for (SelectItem item : selectItems) {
			if (item instanceof SelectExpressionItem) {
				SelectExpressionItem seItem = (SelectExpressionItem) item;
				if (seItem.getExpression() instanceof Function) {
					ColumnDefinition cd = new ColumnDefinition();
					ColDataType type = new ColDataType();
					type.setDataType("decimal");
					cd.setColDataType(type);
					if (seItem.getAlias() == null) {
						Function fn = (Function) seItem.getExpression();
						tupleSchema.put(fn.toString(), count++);
						schema.put(fn.toString(), cd);
					} else {
						tupleSchema.put(seItem.getAlias(), count++);
						schema.put(seItem.getAlias(), cd);
					}

				}
			}
		}
		this.tableInfo.setSchema(schema);
		this.tableInfo.setTupleSchema(tupleSchema);

		ArrayList<ArrayList<String>> output = new ArrayList<ArrayList<String>>();
		for (ArrayList<String> tempTuple : o) {
			this.setTuple(tempTuple);
			ArrayList<String> temp = new ArrayList<String>();

			/**
			 * For each item in the select clause, the schema is modified and
		     * the appropriate schema is updated.
			 */
			for (SelectItem item : selectItems) {
				item.accept(new SelectItemVisitor() {

					@Override
					public void visit(SelectExpressionItem arg0) {
						// TODO Auto-generated method stub
						if (arg0.getExpression() instanceof Function) {
							String s;
							if (arg0.getAlias() == null)
								s = arg0.getExpression().toString();
							else
								s = arg0.getAlias();
							int colID = tupleSchema.get(s);
							temp.add(getTuple().get(colID));
						} else {
							if (tempTuple != null && tempTuple.size() != 0) {
								try {
									temp.add(eval(arg0.getExpression())
											.toString());
								} catch (SQLException e) {
									e.printStackTrace();
								}
							}
						}
					}

					@Override
					public void visit(AllTableColumns arg0) {
						// TODO Auto-generated method stub

					}

					@Override
					public void visit(AllColumns arg0) {
						// TODO Auto-generated method stub

					}
				});
			}
			output.add(temp);
		}

		HashMap<String, Integer> tupleSchema1 = new HashMap<String, Integer>();
		HashMap<String, ColumnDefinition> schema1 = new HashMap<String, ColumnDefinition>();
		count = 0;
		
		/*
		 * For each item in the select clause, the set of tuples is modified and
		 * the appropriate columns are grouped.
		 */
		for (SelectItem item : selectItems) {
			item.accept(new SelectItemVisitor() {

				@Override
				public void visit(SelectExpressionItem arg0) {
					ColumnDefinition colDef;
					if (arg0.getExpression() instanceof Function) {
						if (arg0.getAlias() == null) {
							colDef = schema
									.get(arg0.getExpression().toString());
							tupleSchema1.put(arg0.getExpression().toString(),
									tupleSchema1.size());
							schema1.put(arg0.getExpression().toString(), colDef);
						} else {
							colDef = schema.get(arg0.getAlias());
							tupleSchema1.put(arg0.getAlias(),
									tupleSchema1.size());
							schema1.put(arg0.getAlias(), colDef);
						}
					} else {
						Expression e = arg0.getExpression();
						if (e instanceof Column) {
							String colName = ((Column) e).getColumnName();
							colDef = schema
									.get(arg0.getExpression().toString());
							tupleSchema1.put(colName, tupleSchema1.size());
							schema1.put(colName, colDef);
						} else {

							colDef = schema
									.get(arg0.getExpression().toString());
							tupleSchema1.put(arg0.getExpression().toString(),
									tupleSchema1.size());
							schema1.put(arg0.getExpression().toString(), colDef);
						}
					}
				}

				@Override
				public void visit(AllTableColumns arg0) {

				}

				@Override
				public void visit(AllColumns arg0) {

				}
			});
			this.tableInfo.setSchema(schema1);
			this.tableInfo.setTupleSchema(tupleSchema1);
		}

		// System.out.println(output);
		return output;
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

	/**
	 * This function evaluates the column based on the 
	 * type of data present in it. 
	 * @param Column arg0
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
			value = new StringValue(tuple.get(colID));
		else if (dataType.contains("date"))
			value = new DateValue(" " + tuple.get(colID) + " ");

		return value;
	}

	@Override
	public ArrayList<String> getNext() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public List<Column> getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(List<Column> groupBy) {
		this.groupBy = groupBy;
	}
}
