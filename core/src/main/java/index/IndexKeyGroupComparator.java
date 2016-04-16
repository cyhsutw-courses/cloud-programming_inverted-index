package index;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IndexKeyGroupComparator extends WritableComparator {
	public IndexKeyGroupComparator() {
		super(IndexKey.class, true);
	}

	@Override
	public int compare(WritableComparable lhs, WritableComparable rhs) {
		IndexKey left = (IndexKey) lhs;
		IndexKey right = (IndexKey) rhs;

		return left.getDocName().compareTo(right.getDocName());
	}
}
