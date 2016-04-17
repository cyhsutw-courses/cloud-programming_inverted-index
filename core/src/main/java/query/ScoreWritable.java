package query;

import index.LongArrayWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ScoreWritable implements Writable {

	private Text term;
	private DoubleWritable invertedDocumentFreq;
	private LongArrayWritable offsets;

	public ScoreWritable() {
		term = new Text();
		invertedDocumentFreq = new DoubleWritable();
		offsets = new LongArrayWritable();
	}

	public ScoreWritable(String term, double invertedDocumentFreq,
			List<Long> offsets) {
		this.term = new Text(term);
		this.invertedDocumentFreq = new DoubleWritable(invertedDocumentFreq);
		LongWritable[] tmpArr = new LongWritable[offsets.size()];
		for (int i = 0; i < offsets.size(); i += 1) {
			tmpArr[i] = new LongWritable(offsets.get(i));
		}
		this.offsets = new LongArrayWritable(tmpArr);
	}

	public String getTerm() {
		return term.toString();
	}

	public List<Long> getOffsets() {
		return offsets.asList();
	}

	public double getInvertedDocumentFrequency() {
		return invertedDocumentFreq.get();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		term.write(out);
		invertedDocumentFreq.write(out);
		offsets.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		term.readFields(in);
		invertedDocumentFreq.readFields(in);
		offsets.readFields(in);
	}

}
