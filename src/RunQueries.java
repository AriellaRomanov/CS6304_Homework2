import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.mapreduce.RowCounter;


public class RunQueries {
	public static String Table_Name = "HBase";
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Throwable {
		System.out.println("Starting analytics....");
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		System.out.println("Table aquired.");
		
		// define the filter for Product:Score
		String _product_id = "B003AI2VGA";
		SingleColumnValueFilter filter0 = new SingleColumnValueFilter(
				Bytes.toBytes("Product"), 
				Bytes.toBytes("ProductId"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(_product_id)));

		Scan scan0 = new Scan();
		scan0.setFilter(filter0);
		System.out.println("Starting scan....");
		ResultScanner scanner0 = hTable.getScanner(scan0); 

		System.out.println("First 10 sorted users who reviewed Product ID[" + _product_id + "]:");
		int _i = 0;
		for(Result result=scanner0.next(); _i < 10 && result!=null; result=scanner0.next())
		{
			_i++;
			String text = new String(result.getValue(Bytes.toBytes("User"),Bytes.toBytes("UserId")));
			System.out.println("\t" + text);
		}
		
		//versioning
		String row_key = "B003AI2VGA.A141HP4LYPWMSR";
		//initialize a put with row key
		Put put = new Put(Bytes.toBytes(row_key));

		//initialize a ge with row key
		Get get = new Get(Bytes.toBytes(row_key));
		get.setMaxVersions(3);

		//insert additional data
		put.add(Bytes.toBytes("Product"), Bytes.toBytes("Summary"), Bytes.toBytes("MOAR NextNew sUmMmArY"));
		hTable.put(put);

		Result versions = hTable.get(get);
		List<KeyValue> allResult = versions.getColumn(Bytes.toBytes("Product"), Bytes.toBytes("Summary"));
		System.out.println("Version History of Summary for RowKey(" + row_key + "): ");
		for(KeyValue kv: allResult) {
			System.out.println('\t' + new String(kv.getValue()));
		}
		
		
		// get row count
		int rows = 0; //7831442;
		if (rows == 0)
		{
			ResultScanner scanner = hTable.getScanner(new Scan());
			for (Result rs = scanner.next(); rs != null; rs = scanner.next()) rows++;
		}
		System.out.println("Number of rows in table:                 " + rows);
		
		
		//define the filter for Product:Helpfulness
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes("Product"), 
				Bytes.toBytes("Helpfulness"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("7/7")));

		Scan scan1 = new Scan();
		scan1.setFilter(filter1);
		ResultScanner scanner1 = hTable.getScanner(scan1);
		
		int helpNo = 0;
		for(Result result=scanner1.next(); result!=null; result=scanner1.next()) helpNo++;
		System.out.println("Reviews with Helpfulness == 7/7:         " + helpNo);
		
		// define the filter for Product:Score
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes("Product"), 
				Bytes.toBytes("Score"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("3.0")));

		Scan scan2 = new Scan();
		scan2.setFilter(filter2);
		ResultScanner scanner2 = hTable.getScanner(scan2); 

		int scoreNo = 0;
		for(Result result=scanner2.next(); result!=null; result=scanner2.next()) scoreNo++;
		System.out.println("Reviews with Scores == 3.0:              " + scoreNo);
		
		// define the filter for Product:Text
		SingleColumnValueFilter filter3 = new SingleColumnValueFilter(
				Bytes.toBytes("Product"), 
				Bytes.toBytes("Text"),
				CompareOp.EQUAL,
				new SubstringComparator("the "));

		Scan scan3 = new Scan();
		scan3.setFilter(filter3);
		ResultScanner scanner3 = hTable.getScanner(scan3); 
		
		double count3 = 0;
		for(Result result=scanner3.next(); result!=null; result=scanner3.next())
		{
			String text = new String(result.getValue(Bytes.toBytes("Product"),Bytes.toBytes("Text")));
			int c = StringUtils.countMatches(text, "the ");
			count3 += c;
		}
		System.out.println("Average instances of 'the' in Text:      " + (count3 / rows));
		
		// define the filter for Product:Summary
		SingleColumnValueFilter filter4 = new SingleColumnValueFilter(
				Bytes.toBytes("Product"), 
				Bytes.toBytes("Summary"),
				CompareOp.EQUAL,
				new SubstringComparator("!"));

		Scan scan4 = new Scan();
		scan3.setFilter(filter4);
		ResultScanner scanner4 = hTable.getScanner(scan4); 

		double count4 = 0;
		for(Result result=scanner4.next(); result!=null; result=scanner4.next())
		{
			String text = new String(result.getValue(Bytes.toBytes("Product"),Bytes.toBytes("Summary")));
			int c = StringUtils.countMatches(text, "!");
			count4 += c;
		}
		System.out.println("Average instances of '!' in Summary:     " + (count4 / rows));
	}
}
