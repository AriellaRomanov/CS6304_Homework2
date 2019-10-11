import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InsertData extends Configured implements Tool{

	public String Table_Name = "HBase";
    @SuppressWarnings("deprecation")
	@Override
    public int run(String[] argv) throws IOException {
        Configuration conf = HBaseConfiguration.create();        
        @SuppressWarnings("resource")
		HBaseAdmin admin=new HBaseAdmin(conf);        
        
        boolean isExists = admin.tableExists(Table_Name);
        
        if(isExists == false) {
	        //create table with column family
	        HTableDescriptor htb=new HTableDescriptor(Table_Name);
	        HColumnDescriptor UserFamily = new HColumnDescriptor("User");
	        HColumnDescriptor ProductFamily = new HColumnDescriptor("Product");
	        ProductFamily.setMaxVersions(3);
	        
	        htb.addFamily(UserFamily);
	        htb.addFamily(ProductFamily);
	        admin.createTable(htb);
        }
        
        try {
    		@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new FileReader("movies.txt"));
    	    String line;
    	    
    	    int row_count = 0;
    	    String productId = "";
    	    String userId = "";
    	    String profileName = "";
    	    String helpfulness = "";
    	    String score = "";
    	    String time = "";
    	    String summary = "";
    	    String text = "";
    	    
    	    
    	    //iterate over every line of the input file
    	    while((line = br.readLine()) != null) {
    	    	
    	    	if(line.isEmpty()) continue;

    	    	String[] lineArray = line.split(": ", 2);
    	    	if (lineArray.length >= 2)
    	    	{
    	    		String data_id = lineArray[0];
	    	    	String data_value = lineArray[1];
	    	    	
	    	    	if (data_id.contains("productId"))
	    	    		productId = data_value;
	    	    	else if (data_id.contains("userId"))
	    	    		userId = data_value;
	    	    	else if (data_id.contains("profileName"))
	    	    		profileName = data_value;
	    	    	else if (data_id.contains("helpfulness"))
	    	    		helpfulness = data_value;
	    	    	else if (data_id.contains("score"))
	    	    		score = data_value;
	    	    	else if (data_id.contains("time"))
	    	    		time = data_value;
	    	    	else if (data_id.contains("summary"))
	    	    		summary = data_value;
	    	    	else if (data_id.contains("text"))
	    	    	{
	    	    		text = data_value;
	    	    		
		    	    	//initialize a put with row key as number
	    	    		String row_key = productId + "." + userId;
			            Put put = new Put(Bytes.toBytes(row_key));
	    	    		
	    	    		if (row_count % 10000 == 0)
	    	    			System.out.println(row_key);
	    	    		row_count++;
			            
			            //add column data one after one
			            put.add(Bytes.toBytes("User"), Bytes.toBytes("UserId"), Bytes.toBytes(userId));
			            put.add(Bytes.toBytes("User"), Bytes.toBytes("ProfileName"), Bytes.toBytes(profileName));
			            
			            put.add(Bytes.toBytes("Product"), Bytes.toBytes("ProductId"), Bytes.toBytes(productId));
			            put.add(Bytes.toBytes("Product"), Bytes.toBytes("Helpfulness"), Bytes.toBytes(helpfulness));
			            put.add(Bytes.toBytes("Product"), Bytes.toBytes("Score"), Bytes.toBytes(score));
			            put.add(Bytes.toBytes("Product"), Bytes.toBytes("Time"), Bytes.toBytes(time));
			            put.add(Bytes.toBytes("Product"), Bytes.toBytes("Summary"), Bytes.toBytes(summary));
			            put.add(Bytes.toBytes("Product"), Bytes.toBytes("Text"), Bytes.toBytes(text));
			            
			            //add the put in the table
		    	    	HTable hTable = new HTable(conf, Table_Name);
		    	    	hTable.put(put);
		    	    	hTable.close();
	    	    	}
    	    	}
	    	}
    	    System.out.println("Inserted " + row_count + " Inserted");
    	    
	    } catch (FileNotFoundException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } catch (IOException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } 

      return 0;
   }
    
    public static void main(String[] argv) throws Exception {
        int ret = ToolRunner.run(new InsertData(), argv);
        System.exit(ret);
    }
}