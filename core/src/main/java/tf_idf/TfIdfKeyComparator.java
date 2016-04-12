package tf_idf;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TfIdfKeyComparator extends WritableComparator {
	public TfIdfKeyComparator() {
		super(TfIdfKey.class, true);
	}

	@Override
	public int compare(WritableComparable lhs, WritableComparable rhs) {
		TfIdfKey left = (TfIdfKey) lhs;
		TfIdfKey right = (TfIdfKey) rhs;

		return left.compareTo(right);
	}
}
