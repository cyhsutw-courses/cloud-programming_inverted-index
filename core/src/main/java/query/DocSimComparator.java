package query;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DocSimComparator extends WritableComparator {
	public DocSimComparator() {
		super(DocumentSimilarityPair.class, true);
	}

	@Override
	public int compare(WritableComparable lhs, WritableComparable rhs) {
		DocumentSimilarityPair left = (DocumentSimilarityPair) lhs;
		DocumentSimilarityPair right = (DocumentSimilarityPair) rhs;

		int result = Double
				.compare(left.getSimilarity(), right.getSimilarity());
		if (result == 0) {
			result = left.getDocName().compareTo(right.getDocName());
		}
		return result;
	}
}
