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
		
		
		// Use Nested Loops Join Algorithm
		while(left.hasNext()){
			final Tuple lt = left.getNext();
			
		
			while(right.hasNext()){
				final Tuple rt = right.getNext();
				
				final Tuple newT = Tuple.join(lt, rt, schema);
				
				boolean selected = true;
				
				/**
				 * Assume predicates are connected by AND
				 */
				for(Predicate pred : preds) {
					if(!pred.evaluate(newT)){
						selected = false;
						break;
					}
				}
				
				if(selected){
					result.add(newT);
				}
				
			} // end inner loop
			
			// RESET right iterator!
			right.restart();
		} // end outer loop
		
		// Close child iterators (no longer needed)
		left.close();
		right.close();
		
		// Set up iterator of the result set
		iter = result.iterator();
		
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
		iter = result.iterator();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return iter != null;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		iter = null;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		return isOpen() ? iter.hasNext() : false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
		
		if(!iter.hasNext()) {
			throw new IllegalStateException();
		}
		
		return iter.next();
	}

} // public class SimpleJoin extends Iterator
