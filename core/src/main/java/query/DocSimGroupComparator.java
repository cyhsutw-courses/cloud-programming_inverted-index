package query;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DocSimGroupComparator extends WritableComparator {
	public DocSimGroupComparator() {
		super(DocumentSimilarityPair.class, true);
	}

	@Override
	public int compare(WritableComparable lhs, WritableComparable rhs) {
		DocumentSimilarityPair left = (DocumentSimilarityPair) lhs;
		DocumentSimilarityPair right = (DocumentSimilarityPair) rhs;

		return left.getDocName().compareTo(right.getDocName());
	}
}
