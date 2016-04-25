package score;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DocSimSortComparator extends WritableComparator {
	public DocSimSortComparator() {
		super(DocSimPair.class, true);
	}

	@Override
	public int compare(WritableComparable lhs, WritableComparable rhs) {
		DocSimPair left = (DocSimPair) lhs;
		DocSimPair right = (DocSimPair) rhs;

		int result = Double
				.compare(right.getSimilarity(), left.getSimilarity());
		if (result == 0) {
			result = left.getDocName().compareTo(right.getDocName());
		}
		return result;
	}
}
