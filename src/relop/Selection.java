package relop;

import java.util.HashSet;
import java.util.Set;
import java.util.Iterator;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends relop.Iterator {
	
	// Query tree iterator
	private final relop.Iterator tIter;
	
	// A set of selected tuples
	private final Set<Tuple> result;
	
	// iterator of result set
	private Iterator<Tuple> rIter;
	
	/**
	 * Constructs a selection, given the underlying iterator and predicates.
	 */
	public Selection(relop.Iterator iter, Predicate... preds) {
		// copy iter's schema 
		schema = iter.schema;
		
		tIter = iter;
		
		/**
		 * Build a result set.
		 * Do tree traversal only once to save operations cost.
		 */
		result = new HashSet<Tuple>();

		// iterate through each tuple
		while(iter.hasNext()) {
			
			final Tuple tuple = iter.getNext();
			
			// iterate through predicate
			for(Predicate pred : preds){
				
				// evaluate the tuple
				if(pred.evaluate(tuple)) {
					
					result.add(tuple);
					/// break as soon as the tuple satisfy predicate
					break;
				}
			}
			
		}
		
		// close tree iterator
		iter.close();
		
		// Initialize selection iterator
		rIter = result.iterator();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		// indent
		indent(depth);
		
		// print one-line explanation
		System.out.println("Selection iterator");
		
		// explain child iterator
		tIter.explain(depth + 1);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		rIter = result.iterator();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return rIter != null;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		rIter = null;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		return isOpen() ? rIter.hasNext() : false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
		if(!hasNext()){
			throw new IllegalStateException();
		}
		return rIter.next();
	}

} // public class Selection extends Iterator
