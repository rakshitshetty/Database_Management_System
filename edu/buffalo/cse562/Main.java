package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Top;

/**
 * @author Rakshit Main class
 *
 */
public class Main {
	public static void main(String args[]) {
		File dataDir = null;
		File swapDir = null;
		ArrayList<File> sqlFiles = new ArrayList<File>();
		HashMap<String, TableInfo> tables = new HashMap<String, TableInfo>();
		/*
		 * Reads from the data directory and the swap directory.
		 */
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("--data")) {
				dataDir = new File(args[i + 1]);
				i++;
			} else if (args[i].equals("--swap")) {
				swapDir = new File(args[i + 1]);
				i++;
			} else {
				sqlFiles.add(new File(args[i]));
			}
		}

		/**
		 * For every query present, reads the query, parses the query into a
		 * tree and evaluates the tree
		 */
		for (File sqlFile : sqlFiles) {
			try {
				FileReader stream = new FileReader(sqlFile);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement statement;
				while ((statement = parser.Statement()) != null) {
					// create statement
					if (statement instanceof CreateTable) {
						CreateTable ct = (CreateTable) statement;
						TableInfo tableInfo = new TableInfo();
						tableInfo.setTable(ct.getTable());
						List columnDef = ct.getColumnDefinitions();
						HashMap<String, Integer> tupleSchema = new HashMap<String, Integer>();
						HashMap<String, ColumnDefinition> schema = new HashMap<String, ColumnDefinition>();
						int count = 0;
						for (Object o : columnDef) {
							ColumnDefinition colDef = (ColumnDefinition) o;
							tupleSchema.put(colDef.getColumnName(), count++);
							schema.put(colDef.getColumnName(), colDef);
						}
						tableInfo.setTupleSchema(tupleSchema);
						tableInfo.setSchema(schema);
						tables.put(ct.getTable().getName().toLowerCase(),
								tableInfo);
					}
					/**
					 * Select statments.
					 */
					else if (statement instanceof Statement) {
						PlainSelect select = (PlainSelect) ((Select) statement)
								.getSelectBody();

						// from
						FromItem fromItem = select.getFromItem();
						String from = fromItem.toString();
						String alias = fromItem.getAlias();

						if (alias != null) {
							from = from.substring(0, from.indexOf(" AS"));
							tables.get(from).getTable().setAlias(alias);
						}

						ArrayList<Expression> where = splitAndClauses(select
								.getWhere());
						RelationOperator relationOperator = new RelationOperator();
						relationOperator.setDataDir(dataDir);
						relationOperator.setWhere(where);
						fromItem.accept(new FromItemVisitor() {

							@Override
							public void visit(SubJoin arg0) {

							}

							@Override
							public void visit(SubSelect arg0) {

							}

							@Override
							public void visit(Table arg0) {
								// TODO Auto-generated method stub
								relationOperator.setRelation(arg0.getName());
							}
						});

						/**
						 * Joins
						 */
						List<Join> joins = select.getJoins();
						CrossProductOperator cpOperator = null;
						HashJoin joinOperator = null;

						if (joins == null) {
							if (alias != null) {
								TableInfo info = tables.get(from);
								HashMap<String, ColumnDefinition> schema = info
										.getSchema();
								HashMap<String, Integer> tupleSchema = info
										.getTupleSchema();
								HashMap<String, ColumnDefinition> oldSchema = new HashMap<String, ColumnDefinition>();
								HashMap<String, Integer> oldTupleSchema = new HashMap<String, Integer>();
								oldSchema.putAll(schema);
								oldTupleSchema.putAll(tupleSchema);
								info.setOldSchema(oldSchema);
								info.setOldTupleSchema(oldTupleSchema);
								schema.clear();
								tupleSchema.clear();
								for (Entry<String, ColumnDefinition> entry : info
										.getOldSchema().entrySet()) {
									String newColName = alias + "."
											+ entry.getKey();
									ColumnDefinition newCD = entry.getValue();
									newCD.setColumnName(newColName);
									schema.put(newColName, newCD);
								}
								int size = info.getOldTupleSchema().size();
								for (Entry<String, Integer> entry : info
										.getOldTupleSchema().entrySet()) {
									String newColName = alias + "."
											+ entry.getKey();
									Integer newColID = entry.getValue();
									tupleSchema.put(newColName, newColID);
								}
								info.setTupleSchema(tupleSchema);
								info.setSchema(schema);

							}
						} else {

							joinOperator = new HashJoin();
							joinOperator.setRelationList(joins);
							joinOperator.setWhere(where);
							TableInfo info = joinOperator.join(tables,
									relationOperator);
							from = "join";
							tables.put(from, info);
						}

						/**
						 * Where clause
						 */
						SelectOperator selectOperator = new SelectOperator();
						// System.out.println(where);
						if (where.size() > 0) {
							Expression exp = createExpression(where);
							selectOperator.setWhere(exp);
							selectOperator.setTableInfo(tables.get(from));
							where = null;
						}

						/**
						 * Select clause
						 */
						List<SelectItem> selectItems = select.getSelectItems();
						ProjectOperator projectOperator = new ProjectOperator();
						projectOperator.setTableInfo(tables.get(from));
						projectOperator.setSelectItems(selectItems);

						/**
						 * Setting up the tree.
						 */
						relationOperator.setChild(null);
						relationOperator.setParent(selectOperator);
						selectOperator.setParent(projectOperator);
						/*
						 * if(cpOperator == null)
						 * selectOperator.setChild(relationOperator); else
						 * selectOperator.setChild(cpOperator);
						 */
						if (joinOperator == null)
							selectOperator.setChild(relationOperator);
						else
							selectOperator.setChild(joinOperator);
						projectOperator.setParent(null);
						projectOperator.setChild(selectOperator);

						/**
						 * Group By
						 */
						List<Column> groupBy = select
								.getGroupByColumnReferences();
						projectOperator.setGroupBy(groupBy);

						ArrayList<String> tuple;
						ArrayList<ArrayList<String>> output = new ArrayList<ArrayList<String>>();
						while ((tuple = projectOperator.getNext()) != null) {
							output.add(tuple);
						}

						if (groupBy != null) {
							ArrayList<ArrayList<String>> newOutput = new ArrayList<ArrayList<String>>();
							for (ArrayList<String> list : output) {
								ArrayList<String> temp = new ArrayList<String>();
								for (String s : list) {
									if (s.equals("^!$")) {
										newOutput.add(temp);
										temp = new ArrayList<String>();
									} else
										temp.add(s);
								}
							}
							output = newOutput;

							GroupByOperator gbOperator = new GroupByOperator();
							gbOperator.setSelectItems(selectItems);
							gbOperator.setTableInfo(projectOperator
									.getTableInfo());
							gbOperator.setGroupBy(groupBy);
							output = gbOperator.output(output);

							/**
							 * Order by
							 */
							List<OrderByElement> orderBy = select
									.getOrderByElements();
							if (orderBy != null) {
								OrderByOperator obOperator = new OrderByOperator();
								obOperator.setOrderBy(orderBy);
								obOperator.setTableInfo(gbOperator
										.getTableInfo());
								output = obOperator.order(output);
							}
						}

						Limit limit = select.getLimit();
						long rowCount = -1;
						if (limit != null)
							rowCount = limit.getRowCount();

						display(output, rowCount);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This function displays the ouput as in the result of the query that has
	 * been evaluated.
	 * 
	 * @param output
	 * @param rowCount
	 */
	public static void display(ArrayList<ArrayList<String>> output,
			long rowCount) {
		for (ArrayList<String> tuple : output) {
			if (rowCount == 0)
				break;
			int size = tuple.size();
			if (size > 0) {
				for (int i = 0; i < size; i++) {
					String s = tuple.get(i);
					try {
						s = new DecimalFormat("0.0000").format(Double
								.parseDouble(s));
					} catch (NumberFormatException e) {
						s = tuple.get(i);
					}

					if (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'')
						s = s.substring(1, s.length() - 1);
					if (i < size - 1)
						System.out.print(s + "|");
					else
						System.out.print(s);
				}
				System.out.println();
			}
			rowCount--;
		}
	}

	/**
	 * This function splits the clauses and adds them into a list and returns
	 * that list.
	 * 
	 * @param e
	 * @return
	 */
	public static ArrayList<Expression> splitAndClauses(Expression e) {
		ArrayList<Expression> ret = new ArrayList<Expression>();
		if (e instanceof AndExpression) {
			AndExpression a = (AndExpression) e;
			ret.addAll(splitAndClauses(a.getLeftExpression()));
			ret.addAll(splitAndClauses(a.getRightExpression()));
		} else {
			ret.add(e);
		}

		return ret;
	}

	/**
	 * Creates an expression object from the list of expressions taken as the
	 * parameter.
	 * 
	 * @param where
	 * @return
	 */
	public static Expression createExpression(ArrayList<Expression> where) {
		int size = where.size();
		while (size > 1) {
			ArrayList<Expression> expList = new ArrayList<Expression>();
			if (size % 2 == 0) {
				for (int i = 0; i < size; i = i + 2) {
					AndExpression a = new AndExpression(where.get(i),
							where.get(i + 1));
					expList.add(a);
				}
			} else {
				for (int i = 0; i < size - 1; i = i + 2) {
					AndExpression a = new AndExpression(where.get(i),
							where.get(i + 1));
					expList.add(a);
				}
				expList.add(where.get(size - 1));
			}
			where = expList;
			size = where.size();
		}
		return where.get(0);
	}
}
