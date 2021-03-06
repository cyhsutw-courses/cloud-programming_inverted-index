package score;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DocSimPair implements WritableComparable<DocSimPair>{

	private Text docName;
	private DoubleWritable similarity;

	public DocSimPair() {
		this.docName = new Text();
		this.similarity = new DoubleWritable();
	}

	public DocSimPair(String docName, double similarity) {
		this.docName = new Text(docName);
		this.similarity = new DoubleWritable(similarity);
	}

	public String getDocName() {
		return docName.toString();
	}

	public double getSimilarity() {
		return similarity.get();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		docName.write(out);
		similarity.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		docName.readFields(in);
		similarity.readFields(in);
	}

	@Override
	public int compareTo(DocSimPair o) {
		int result = this.docName.toString().compareTo(o.docName.toString());
		if (result == 0) {
			// return Double.compare(this.similarity.get(), o.similarity.get());
		}
		return result;
	}

}
