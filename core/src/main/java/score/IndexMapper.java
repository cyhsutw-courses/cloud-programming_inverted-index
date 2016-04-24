package score;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.alibaba.fastjson.JSON;


public class IndexMapper extends Mapper<LongWritable, Text, DocSimPair, Text> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		boolean isOrQuery = context.getConfiguration().getBoolean("query.or", false);
		List<String> docNames = Arrays.asList(context.getConfiguration().getStrings("inputFiles"));
		for (String rawVal : value.toString().split("\n")){
			String[] kv = rawVal.toString().split("\\|");
			String[] docSim = kv[0].split("::");
			DocSimPair ds = new DocSimPair(docNames.get(Integer.parseInt(docSim[0])), Double.parseDouble(docSim[1]));

			String[] kwOffsets = kv[1].split(";");

			Map<String, List<Long>> kwOffsetMap = new HashMap<>();

			int maxWordLength = Integer.MIN_VALUE;
			for (String s : kwOffsets) {
				String[] p = s.split(" ");
				List<Long> offs = JSON.parseArray(p[1], Long.class);
				kwOffsetMap.put(p[0], offs);
				if (p[0].length() > maxWordLength) {
					maxWordLength = p[0].length();
				}
			}

			Path inputPath = new Path(context.getConfiguration().get("rawPath") + "/" + ds.getDocName());
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream inStream = fs.open(inputPath);
			StringBuffer buff = new StringBuffer();
			int tmp;
			while((tmp = inStream.read()) != -1) {
				buff.append((char)tmp);
			}
			String fileContent = buff.toString();
			inStream.close();
			fs.close();

			if (isOrQuery) {
				List<Long> offsets = new ArrayList<>();
				for (List<Long> val : kwOffsetMap.values()) {
					offsets.addAll(val);
				}
				offsets.sort(null);

				StringBuilder sbd = new StringBuilder("************************\n");

				for (Long ff : offsets) {
					sbd.append("\t");
					sbd.append(ff);
					sbd.append("\t");
					if (ff >= 10) {
						sbd.append(fileContent.substring((int)(ff - 10), (int)Math.min(fileContent.length(), ff + 10 + maxWordLength)).replaceAll("\\s+", " "));
					} else {
						sbd.append(fileContent.substring(0, (int)Math.min(fileContent.length(), 10 + maxWordLength)).replaceAll("\\s+", " "));
					}
					sbd.append("\n");
				}

				sbd.append("************************\n");
				context.write(ds, new Text(sbd.toString()));
			} else {
				List<List<Long>> vals = new ArrayList<>();
				int minLength = Integer.MAX_VALUE;
				for (List<Long> val : kwOffsetMap.values()) {
					if (val.size() < minLength) {
						minLength = val.size();
					}
					vals.add(val);
				}

				List<List<Long>> base = new ArrayList<>();
				for (Long val : vals.get(0)) {
					List<Long> n = new ArrayList<>();
					n.add(val);
					base.add(n);
				}

				for (int i = 1; i < vals.size(); i += 1){
					base = product(base, vals.get(i));
				}

				StringBuilder sbd = new StringBuilder();
				for(List<Long> comb : base) {
					comb.sort(null);

					int minOffset = comb.get(0).intValue();
					int maxOffset = comb.get(comb.size() - 1).intValue();

					if (maxOffset - minOffset > (maxWordLength + 20) * comb.size()) {
						continue;
					}

					sbd.append("\t");
					sbd.append(minOffset + "..." + maxOffset);
					sbd.append("\t");
					if (minOffset >= 10) {
						sbd.append(fileContent.substring((int)(minOffset - 10), (int) Math.min(fileContent.length(), maxOffset + 10 + maxWordLength)).replaceAll("\\s+", " "));
					} else {
						sbd.append(fileContent.substring(0, (int) Math.min(fileContent.length(), maxOffset + 10 + maxWordLength)).replaceAll("\\s+", " "));
					}
					sbd.append("\n");
				}


				if (sbd.toString().length() > 0) {
					sbd.append("************************\n");
					context.write(ds, new Text("************************\n" + sbd.toString()));
				}

			}
		}

	}

	private List<List<Long>> product(List<List<Long>> a, List<Long> b) {
		List<List<Long>> newVal = new ArrayList<>();
		for (Long e : b) {
			for (List<Long> list : a) {
				List<Long> newL = new ArrayList<>(list);
				newL.add(e);
				newVal.add(newL);
			}
		}
		return newVal;
	}
}
