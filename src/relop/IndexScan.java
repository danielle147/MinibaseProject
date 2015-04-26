package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

	private final HeapFile file;

	private final HashIndex index;

	private BucketScan bscan;
	
	private SearchKey lastKey;

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	public IndexScan(Schema schema, HashIndex index, HeapFile file) {
		this.schema = schema;

		this.index = index;
		this.file = file;
		
		init();
	}

	private void init() {
		bscan = index.openScan();
		lastKey = null;
	}
	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		// Indent first
		indent(depth);

		// Print one line explanation
		System.out.println("IndexScan iterator " +
				"[Index file=" + index + 
				", Heap file=" + file);

		/** It seems that we have no child iterators for now */
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
		return bscan != null;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		if(isOpen()) {
			bscan.close();
			bscan = null;
			lastKey = null;
		}
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		return isOpen() ? bscan.hasNext() : false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException if no more tuples
	 */
	public Tuple getNext() {
		
		final RID rid = bscan.getNext();
		
		// get the key of last record
		lastKey = bscan.getLastKey();
		
		return new Tuple(schema, file.selectRecord(rid));
	}

	/**
	 * Gets the key of the last tuple returned.
	 */
	public SearchKey getLastKey() {
		return lastKey;
	}

	/**
	 * Returns the hash value for the bucket containing the next tuple, or maximum
	 * number of buckets if none.
	 */
	public int getNextHash() {
		if(!isOpen()) {
			throw new IllegalStateException();
			
		}
		return bscan.getNextHash();
	}

} // public class IndexScan extends Iterator
