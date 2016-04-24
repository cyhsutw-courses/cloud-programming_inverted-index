package query;

import org.apache.hadoop.mapreduce.Partitioner;

public class DocSimPartitioner extends Partitioner<DocumentSimilarityPair, ScoreArrayWritable> {

	@Override
	public int getPartition(DocumentSimilarityPair arg0, ScoreArrayWritable arg1, int arg2) {
		return 0;
	}

}
