package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

	private final HeapFile file; 

	private HeapScan hscan;

	private RID lastRID;

	/**
	 * Constructs a file scan, given the schema and heap file.
	 */
	public FileScan(Schema schema, HeapFile file) {
		this.schema = schema;
		this.file = file;  
		init();
	}

	private void init() {
		hscan = file.openScan();
		lastRID = null;
	}

	/**
	 * Gives a one-line explanation of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		// Indent first
		indent(depth);

		// Print one line explanation
		System.out.println("FileScan iterator [Heap file=" + file + "]");

		/** It seems that we have no child iterators for now */
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {

		close();
		
		// Re-initialize
		init();
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return hscan != null;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		if(isOpen()) {
			
			// Close heap scan
			hscan.close();
			hscan = null;
			lastRID = null;
		}	
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		
		return isOpen() ? hscan.hasNext() : false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
		lastRID = new RID();
		
		return new Tuple(schema, hscan.getNext(lastRID));
	}

	/**
	 * Gets the RID of the last tuple returned.
	 */
	public RID getLastRID() {
		return lastRID;
	}

} // public class FileScan extends Iterator
