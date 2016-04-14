package index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TermFrequencyWritable implements Writable {

	private Text docName;
	private IntWritable frequency;
	private ArrayWritable offsets;

	public TermFrequencyWritable() {
		docName = new Text();
		frequency = new IntWritable();
		offsets = new ArrayWritable(IntWritable.class);
	}

	public TermFrequencyWritable(String docName, List<Integer> offsets) {
		this.docName = new Text(docName);
		ArrayList<IntWritable> objects = new ArrayList<>();
		for (Integer offset : offsets) {
			objects.add(new IntWritable(offset.intValue()));
		}
		this.frequency = new IntWritable(offsets.size());
		IntWritable[] arr = new IntWritable[offsets.size()];
		this.offsets = new ArrayWritable(IntWritable.class,
				objects.toArray(arr));
	}

	public String getDocName() {
		return docName.toString();
	}

	public Integer[] getOffsets() {
		List<IntWritable> offsets = Arrays.asList(((IntWritable[]) this.offsets
				.toArray()));
		return (Integer[]) offsets.stream().map(o -> new Integer(o.get()))
				.toArray();
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

}
