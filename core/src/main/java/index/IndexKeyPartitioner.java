package index;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapreduce.Partitioner;

public class IndexKeyPartitioner extends
		Partitioner<IndexKey, TermFrequencyWritable> {
	private static final List<Character> CHAR_RANGE = Arrays.asList('a', 'b', 'c', 'd', 'e',
			'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
			's', 't', 'u', 'v', 'w', 'x', 'y', 'z');

	@Override
	public int getPartition(IndexKey key, TermFrequencyWritable value,
			int numReduceTasks) {
		char firstChar = key.getTerm().charAt(0);
		int division = CHAR_RANGE.size() / numReduceTasks;
		int index = CHAR_RANGE.indexOf(firstChar);
		int div = index / division;
		return div >= numReduceTasks ? (numReduceTasks - 1) : div;
	}
}
