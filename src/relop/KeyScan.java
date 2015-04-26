package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

	private final HashIndex index;

	private final SearchKey key;

	private final HeapFile file;

	private HashScan scan;

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
		this.schema = schema;

		this.index = index;
		this.key = key;
		this.file = file;

		init();
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		// Indent first
		indent(depth);

		// Print one line explanation
		System.out.println("KeyScan iterator " +
				"[Index file=" + index + 
				", Heap file=" + file);

		/** It seems that we have no child iterators for now */
	}

	private void init() {
		scan = index.openScan(key);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		close();
		init();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return scan != null;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		
		if(isOpen()){
			
			// close the hash scan
			scan.close();
			scan = null;
		}
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		return isOpen() ? scan.hasNext() : false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
	
		// Obtain RID of next matched record
		final RID rid = scan.getNext();
		
		return new Tuple(schema, file.selectRecord(rid));
	}

} // public class KeyScan extends Iterator
