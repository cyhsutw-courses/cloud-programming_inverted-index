package index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TermFrequencyWritable implements Writable {

	private Text docName;
	private IntWritable frequency;
	private LongArrayWritable offsets;

	public TermFrequencyWritable() {
		docName = new Text();
		frequency = new IntWritable();
		offsets = new LongArrayWritable();
	}

	public TermFrequencyWritable(String docName, List<Long> offsets) {
		this.docName = new Text(docName);
		ArrayList<LongWritable> objects = new ArrayList<>();
		for (Long offset : offsets) {
			objects.add(new LongWritable(offset.intValue()));
		}
		this.frequency = new IntWritable(offsets.size());
		LongWritable[] arr = new LongWritable[offsets.size()];
		this.offsets = new LongArrayWritable(objects.toArray(arr));
	}

	public String getDocName() {
		return docName.toString();
	}

	public Long[] getOffsets() {
		Writable[] tmpOffsets = this.offsets.get();
		Long[] returnValue = new Long[tmpOffsets.length];
		for (int i = 0; i < tmpOffsets.length; i += 1) {
			returnValue[i] = new Long(tmpOffsets[i].toString());
		}
		return returnValue;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		docName.readFields(input);
		frequency.readFields(input);
		offsets.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		docName.write(output);
		frequency.write(output);
		offsets.write(output);
	}

	@Override
	public String toString() {
		return String.join(" ", docName.toString(), frequency.toString(),
				offsets.toString());
	}
}
