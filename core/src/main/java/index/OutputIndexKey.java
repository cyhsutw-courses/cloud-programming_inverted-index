package index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class OutputIndexKey implements WritableComparable<OutputIndexKey> {

	private Text term;
	private IntWritable docFreq;

	public OutputIndexKey() {
		term = new Text();
		docFreq = new IntWritable();
	}

	public OutputIndexKey(String term, int docFreq) {
		this.term = new Text(term);
		this.docFreq = new IntWritable(docFreq);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		term.readFields(input);
		docFreq.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		term.write(output);
		docFreq.write(output);
	}

	@Override
	public String toString() {
		return term.toString() + " " + docFreq.toString();
	}

	@Override
	public int compareTo(OutputIndexKey o) {
		return term.compareTo(o.term);
	}

}
