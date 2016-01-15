package edu.buffalo.cse562;

import java.util.HashMap;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/**
 * @author Rakshit
 * 
 *         TableInfo defines the schema of a particular relation
 *
 */
public class TableInfo {
	private Table table;
	private HashMap<String, Integer> tupleSchema;
	private HashMap<String, Integer> oldTupleSchema;
	private HashMap<String, ColumnDefinition> schema;
	private HashMap<String, ColumnDefinition> oldSchema;

	public TableInfo() {
		this.table = new Table();
		this.tupleSchema = new HashMap<String, Integer>();
		this.oldTupleSchema = new HashMap<String, Integer>();
		this.schema = new HashMap<String, ColumnDefinition>();
		this.oldSchema = new HashMap<String, ColumnDefinition>();
	}

	public HashMap<String, Integer> getTupleSchema() {
		return tupleSchema;
	}

	public void setTupleSchema(HashMap<String, Integer> tupleSchema) {
		this.tupleSchema = tupleSchema;
	}

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public HashMap<String, ColumnDefinition> getSchema() {
		return schema;
	}

	public void setSchema(HashMap<String, ColumnDefinition> schema) {
		this.schema = schema;
	}

	public HashMap<String, Integer> getOldTupleSchema() {
		return oldTupleSchema;
	}

	public void setOldTupleSchema(HashMap<String, Integer> oldTupleSchema) {
		this.oldTupleSchema = oldTupleSchema;
	}

	public HashMap<String, ColumnDefinition> getOldSchema() {
		return oldSchema;
	}

	public void setOldSchema(HashMap<String, ColumnDefinition> oldSchema) {
		this.oldSchema = oldSchema;
	}

}
