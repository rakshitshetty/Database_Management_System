package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;

/**
 * @author Rakshit
 * 
 *         Relation operator deals with a Table as a whole.
 * 
 */
public class RelationOperator implements Operator {
	private String relation;
	private ArrayList<String> tuple = new ArrayList<String>();
	private File dataDir;
	private FileReader tableStream;
	private BufferedReader br;
	private Operator child;
	private Operator parent;
	private ArrayList<Expression> where;

	public ArrayList<Expression> getWhere() {
		return where;
	}

	public void setWhere(ArrayList<Expression> where) {
		this.where = where;
	}

	public String getRelation() {
		return relation;
	}

	/**
	 * This function sets the relation of the operator and loads the stream
	 * after reading from the disk.
	 * 
	 * @param relation
	 */
	public void setRelation(String relation) {
		this.relation = relation;
		try {
			tableStream = new FileReader(dataDir.getAbsolutePath()
					+ File.separator + this.relation + ".dat");
			br = new BufferedReader(tableStream);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	/**
	 * This function returns the next tuple in the order of their Occurrence.
	 * 
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<String> getNext() throws SQLException {
		try {
			if (br != null) {
				String tableRow = br.readLine();
				if (tableRow != null) {
					String splitArray[] = tableRow.split("\\|");
					tuple = new ArrayList<String>();
					for (String temp : splitArray) {
						if (!(temp.equals("|")))
							tuple.add(temp);
					}

					return tuple;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public File getDataDir() {
		return dataDir;
	}

	public void setDataDir(File dataDir) {
		this.dataDir = dataDir;
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

}
