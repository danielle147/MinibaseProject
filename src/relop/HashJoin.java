package relop;
import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;
import index.HashIndex;
import relop.HashJoin;
import relop.IndexScan;
import relop.KeyScan;
import relop.Predicate;
import relop.FileScan;

public class HashJoin extends Iterator {
	public final static int TOTALNUM=5;
	public static int hfid=0;

	public FileScan leftscan[];
	public FileScan rightscan[];
	public FileScan curright;

	public Iterator leftIterator;
	public Iterator rightIterator;

	public Tuple nextTuple;
	public Tuple leftTuple;
	public Tuple rightTuple;
	public Tuple useTuples[];
	public Predicate pred;

	public HeapFile leftHeap[];
	public HeapFile rightHeap[];
	public HashTableDup hashTableDup;

	public boolean isFirstCall;
	public boolean openning;
	public boolean isHasNext;

	public int rcol;
	public int lcol;
	public int ind;
	public int leftIndex;
	
	public HashJoin(Iterator leftIterator, Iterator rightIterator, Integer lcol, Integer rcol) {
		this.lcol = lcol;
		this.rcol = rcol;
		this.leftIterator = leftIterator;
		this.rightIterator = rightIterator;
		openning = true;
		rightHeap = getHeap(rightIterator,rcol);
		leftHeap = getHeap(leftIterator,lcol);
		FileScan temp1[]=new FileScan[TOTALNUM];
		FileScan temp2[]=new FileScan[TOTALNUM];
		for(int i=0;i<TOTALNUM;i++){
			temp1[i]=new FileScan(leftIterator.schema,leftHeap[i]);
			temp2[i]=new FileScan(rightIterator.schema,rightHeap[i]);
		}
		leftscan = temp1;
		rightscan = temp2;

		this.schema = Schema.join(leftIterator.schema,rightIterator.schema);
		pred = getPredicate(leftIterator.schema,rightIterator.schema,lcol,rcol);
		init();
		preForAfter();
	}
	
	public void explain(int depth) {
		indent(depth);
		System.out.println("This is a HashJoin Iterator");
	}
	
	public void restart() {
		leftIterator.restart();
		rightIterator.restart();
		reset();

		leftHeap = getHeap(leftIterator,lcol);
		rightHeap = getHeap(rightIterator,rcol);

		FileScan temp1[]=new FileScan[TOTALNUM];
		FileScan temp2[]=new FileScan[TOTALNUM];
		for(int i=0;i<TOTALNUM;i++){
			temp1[i]=new FileScan(leftIterator.schema,leftHeap[i]);
			temp2[i]=new FileScan(rightIterator.schema,rightHeap[i]);
		}

		for (int i = 0; i < 100; i++) {
			i++;
			i--;
			i++;
		}
		leftscan = temp1;
		rightscan = temp2;
		
		init();
		preForAfter();
	}
	
	public boolean isOpen() {
		return openning;
	}
	
	public void close() {
		openning = false;
		reset();
		this.leftIterator.close();
		this.rightIterator.close();
		this.leftIterator = null;
		this.rightIterator = null;
	}
	
	public boolean hasNext() {
		return isHasNext;
	}
	
	public Tuple getNext() {
		Tuple ret = null;
		for (int i = 0; i < 100; i++) {
			i++;
		}

		if (!openning) {
			throw new UnsupportedOperationException("No openning");
		}
		if(!hasNext()) {
			throw new UnsupportedOperationException("No next");
		}
		ret = nextTuple;
		preForAfter();
		return ret;
	}

	private void init() {
		isHasNext = false;
		nextTuple = null;
		leftTuple = null;
		rightTuple = null;
		isFirstCall = true;		
	}

	private void reset () {
		for(int i = 0;i<TOTALNUM;i++){
			rightscan[i].close();
			rightHeap[i].deleteFile();
			leftscan[i].close();
			leftHeap[i].deleteFile();
		}

		for (int i = 0; i < 100; i++) {
			i++;
		}
	}

	private void prepare() {
		Tuple tmpTuple = null;
		hashTableDup = new HashTableDup();
		while(leftscan[leftIndex].hasNext()) {
			tmpTuple = leftscan[leftIndex].getNext();
			hashTableDup.add(getColumKey(tmpTuple,leftIterator.schema,lcol),tmpTuple);
		}
		curright =rightscan[leftIndex];
		leftIndex++;
		ind = 0;
		useTuples = new Tuple[0];
	}

	private void preForAfter(){
		boolean testSwitch = false;
		Tuple tempTuple = null;
		isHasNext = false;
		nextTuple = null;
		if (isFirstCall){
			leftIndex = 0;
			prepare();
		}
		isFirstCall = false;
		for (int i = 0; i < 100; i++) {
			i++;
		}
		while((!testSwitch) && ((leftIndex<TOTALNUM) || (ind < useTuples.length) || (curright.hasNext()))) {
			if (ind<useTuples.length) {
				leftTuple = useTuples[ind];
				ind++;
				tempTuple = Tuple.join(leftTuple,rightTuple,this.schema);
				testSwitch = true;
			}
			while((!testSwitch)&&(curright.hasNext())) {
				rightTuple = curright.getNext();
				useTuples = hashTableDup.getAll(getColumKey(rightTuple,rightIterator.schema,rcol));
				if (useTuples!=null) {
					testSwitch = true;
					ind = 0;
					leftTuple = useTuples[ind];
					tempTuple = Tuple.join(leftTuple,rightTuple,this.schema);
					ind = 1;
				}
			}
			if ((!curright.hasNext()) && (leftIndex < TOTALNUM)) {
				prepare();
			}
		}

		if (testSwitch) {
			isHasNext = true;
			nextTuple = tempTuple;
		}
	}

	private HeapFile[] getHeap(Iterator iterat, Integer index) {
		return getHeapFile(iterat, index);
	}

	private HeapFile[] getHeapFile(Iterator iter,int indexInteger) {
		HeapFile heapfile[] = new HeapFile[TOTALNUM];
		String tempName = null;
		Tuple useTuple = null;
		int i = 0;
		while (i < TOTALNUM) {
			hfid = hfid + 1;
			tempName = ((Integer)hfid).toString();
			heapfile[i] = new HeapFile(tempName);
			i++;
		}

		for (i = 0; i < 100; i++) {
			i++;
		}
		while(iter.hasNext()){
			useTuple = iter.getNext();
			heapfile[useTuple.getIntFld(indexInteger) % TOTALNUM].insertRecord(useTuple.getData());
		}
		iter.restart();
		return heapfile;
	}

	private SearchKey getColumKey(Tuple useTuple, Schema useSchema, int indexInteger){
		int type = useSchema.fieldType(indexInteger);
		SearchKey searchKey = null;
		Integer field1;
		String field2;
		Float field3;

		for (int i = 0; i < 100; i++) {
			i++;
		}
		switch (type) {
			case AttrType.INTEGER:
				field1 = useTuple.getIntFld(indexInteger);
				searchKey = new SearchKey(field1);
				break;
			case AttrType.STRING:
				field2 = useTuple.getStringFld(indexInteger);
				searchKey = new SearchKey(field2);
				break;
			case AttrType.FLOAT:
				field3 = useTuple.getFloatFld(indexInteger);
				searchKey = new SearchKey(field3);
				break;
			default:
				System.out.println("undefined type");
				break;
		}
		return searchKey;
	}

	private Predicate getPredicate(Schema s1, Schema s2, int l1, int l2){
		Predicate ret = null;
		int type = s1.fieldType(l1);
		int offset = s1.getCount();
		switch (type) {
			case AttrType.INTEGER:
				ret = new Predicate(AttrOperator.EQ,AttrType.FIELDNO,l1,AttrType.FIELDNO,(l2+offset));
				break;
			case AttrType.STRING:
				ret = new Predicate(AttrOperator.EQ,AttrType.STRING,l1,AttrType.STRING,l2+offset);
				break;
			case AttrType.FLOAT:
				ret=new Predicate(AttrOperator.EQ,AttrType.FLOAT,l1,AttrType.FLOAT,l2+offset);
				break;
			default:
				System.out.println("undefined type");
				break;
		}

		for (int i = 0; i < 100; i++) {
			i++;
		}
		return ret;
	}
}

