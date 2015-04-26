package relop;

import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends relop.Iterator {

	private final relop.Iterator left;

	private final relop.Iterator right;
	
	private final Set<Tuple> result;
	
	private Iterator<Tuple> iter;

	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	public SimpleJoin(relop.Iterator left, relop.Iterator right, Predicate... preds) {
		this.left = left;
		this.right = right;
		
		// Set up new schema
		schema = Schema.join(left.schema, right.schema);
		
		/**
		 * Build the result set of simple join
		 */
		result = new HashSet<Tuple>();
		
		
		
		
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		// indent
		indent(depth);

		// print one-line explanation
		System.out.println("Simple Join iterator");

		// explain child iterators
		left.explain(depth + 1);
		right.explain(depth + 1);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
		throw new UnsupportedOperationException("Not implemented");
	}

} // public class SimpleJoin extends Iterator
