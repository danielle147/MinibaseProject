package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

	private final Iterator iter;

	private final Integer[] fields;

	/**
	 * Constructs a projection, given the underlying iterator and field numbers.
	 */
	public Projection(Iterator iter, Integer... fields) {

		this.iter = iter;
		this.fields = fields;

		// Set up new schema after projection
		schema = new Schema(fields.length);
		for(int i=0; i < fields.length; i++){
			schema.initField(i, iter.schema, fields[i]);
		}
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		// indent
		indent(depth);

		// print one-line explanation
		System.out.println("Projection iterator");

		// explain child iterator
		iter.explain(depth + 1);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		iter.restart();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return iter.isOpen();
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		iter.close();
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		return iter.hasNext();
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
		
		// Get tuple before projection
		final Tuple oldTuple = iter.getNext();
		
		final Tuple newTuple = new Tuple(schema);
		
		// Project to new one
		for(int i=0; i< fields.length; i++){
			newTuple.setField(i, oldTuple.getField(fields[i]));
		}
		
		return newTuple;
	}

} // public class Projection extends Iterator
