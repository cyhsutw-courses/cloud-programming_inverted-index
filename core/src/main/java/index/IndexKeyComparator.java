package index;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IndexKeyComparator extends WritableComparator {
	public IndexKeyComparator() {
		super(IndexKey.class, true);
	}

	@Override
	public int compare(WritableComparable lhs, WritableComparable rhs) {
		IndexKey left = (IndexKey) lhs;
		IndexKey right = (IndexKey) rhs;

		return left.compareTo(right);
	}
}
