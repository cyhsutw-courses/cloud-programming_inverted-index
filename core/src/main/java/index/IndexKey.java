package index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Objects;

public class IndexKey implements WritableComparable<IndexKey> {

	private Text term;
	private Text docName;

	public IndexKey() {

	}

	public IndexKey(String term, String docName) {
		this.term = new Text(term);
		this.docName = new Text(docName);
	}

	public String getTerm() {
		return term.toString();
	}

	public String getDocName() {
		return docName.toString();
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		term.readFields(input);
		docName.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		term.write(output);
		docName.write(output);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(this.term, this.docName);
	}

	@Override
	public boolean equals(Object k) {
		if (k instanceof IndexKey) {
			IndexKey key = (IndexKey) k;
			return term.equals(key.term) && docName.equals(key.docName);
		}
		return false;
	}

	@Override
	public int compareTo(IndexKey key) {
		int result = term.compareTo(key.term);
		if (result != 0) {
			return result;
		}
		return docName.compareTo(key.docName);
	}

}
