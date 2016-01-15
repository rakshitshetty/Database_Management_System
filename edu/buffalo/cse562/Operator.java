package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;

import net.sf.jsqlparser.expression.LeafValue;

/**
 * @author Rakshit
 * Template for an operator.
 *
 */
public interface Operator {
	public ArrayList<String> getNext() throws SQLException;

	public Operator getParent();

	public void setParent(Operator parent);

	public Operator getChild();

	public void setChild(Operator child);
}
