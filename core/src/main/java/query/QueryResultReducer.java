package query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.codepoetics.protonpack.StreamUtils;

public class QueryResultReducer extends
		Reducer<DocumentSimilarityPair, ScoreArrayWritable, Text, ScoreArrayWritable> {
	@Override
	public void reduce(DocumentSimilarityPair key,
			Iterable<ScoreArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		Configuration config = context.getConfiguration();

		List<String> query = Arrays.asList(config.getStrings("query"));

		List<String> determinedQueryList = new ArrayList<>(new HashSet<>(query));
		determinedQueryList.sort(null);

		int[] queryTf = new int[determinedQueryList.size()];

		for (String term : query) {
			int index = determinedQueryList.indexOf(term);
			queryTf[index] += 1;
		}

		boolean isOrQuery = config.getBoolean("query.or", false);

		double[] vec = new double[determinedQueryList.size()];
		int[] tf = new int[determinedQueryList.size()];
		double[] idf = new double[determinedQueryList.size()];
		List<ScoreWritable> list = new ArrayList<>();
		for (ScoreArrayWritable scoreArray : values) {
			for (Writable rawScore : scoreArray.get()) {
				ScoreWritable score = (ScoreWritable) rawScore;
				int index = determinedQueryList.indexOf(score.getTerm());
				idf[index] = score.getInvertedDocumentFrequency();
				tf[index] = score.getOffsets().size();
				vec[index] = tf[index] * idf[index];
				list.add(score);
			}
		}
		double[] queryVec = new double[determinedQueryList.size()];
		for (int i = 0; i < queryTf.length; i += 1) {
			queryVec[i] = queryTf[i] * idf[i];
		}

		if (isOrQuery) {
			double cosSim = this.calculateSimilarity(vec, queryVec);
			context.write(new Text(key.getDocName() + "::" + Double.toString(cosSim)), new ScoreArrayWritable(list.toArray(new ScoreWritable[list.size()])));
		} else {
			// for "and query"
			for (int i = 0; i < tf.length; i += 1) {
				if (tf[i] == 0) {
					return;
				}
			}
			double cosSim = this.calculateSimilarity(vec, queryVec);

			context.write(new Text(key.getDocName() + "::" + Double.toString(cosSim)), new ScoreArrayWritable(list.toArray(new ScoreWritable[list.size()])));
		}
	}

	private double calculateSimilarity(double[] vec1, double[] vec2) {
		DoubleStream v1s = Arrays.stream(vec1);
		DoubleStream v2s = Arrays.stream(vec2);
		Stream<Double> v1 = v1s.boxed();
		Stream<Double> v2 = v2s.boxed();
		double innerProduct = StreamUtils.zip(v1, v2,
				(a, b) -> new Double(a * b)).reduce(0.0, Double::sum);

		v1s.close();
		v2s.close();
		v1s = Arrays.stream(vec1);
		v2s = Arrays.stream(vec2);
		v1 = v1s.boxed();
		v2 = v2s.boxed();

		double v1Length = Math
				.sqrt(v1.map(e -> e * e).reduce(0.0, Double::sum));
		double v2Length = Math
				.sqrt(v2.map(e -> e * e).reduce(0.0, Double::sum));
		v1s.close();
		v2s.close();

		return innerProduct / (v1Length * v2Length);
	}
}
