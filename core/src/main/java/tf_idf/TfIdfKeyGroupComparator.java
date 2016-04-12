package tf_idf;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TfIdfKeyGroupComparator extends WritableComparator {
	public TfIdfKeyGroupComparator() {
		super(TfIdfKey.class, true);
	}

	@Override
	public int compare(WritableComparable lhs, WritableComparable rhs) {
		TfIdfKey left = (TfIdfKey) lhs;
		TfIdfKey right = (TfIdfKey) rhs;

		return left.term().compareTo(right.term());
	}
}
