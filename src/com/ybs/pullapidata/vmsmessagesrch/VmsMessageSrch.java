package com.ybs.pullapidata.vmsmessagesrch;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.ybs.pullapidata.vmsmessagesrch.ApiConnection;
import com.ybs.pullapidata.vmsmessagesrch.DbConnection;

public class VmsMessageSrch 
{
	static public ApiConnection apiconnection;
	static public String BaseDate, BaseTime;
	static public String tableName = "VMS_MESSAGE_SRCH";
	
	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException 
	{
		// 기본 설정
		long time = System.currentTimeMillis();
		SimpleDateFormat _date = new SimpleDateFormat("YYYYMMdd");
		SimpleDateFormat _time = new SimpleDateFormat("HHmmss");
		BaseDate = _date.format(new Date(time));
		BaseTime = _time.format(new Date( time));
		List<String> column = new ArrayList<String>();
		column.add("seq");
		column.add("vmsId");
		column.add("routeNo");
		column.add("routeName");
		column.add("centerCode");
		column.add("centerName");
		column.add("updownType");
		column.add("vmsMessage");
		column.add("stdLink");
		column.add("xCoord");
		column.add("yCoord");
		
		// DB 연결
		String host = "192.168.0.53";
		String name = "HVI_DB";
		String user = "root";
		String pass = "dlatl#001";
		DbConnection dbconnection = new DbConnection(host, name, user, pass);
	    dbconnection.Connect();
	    String sql = "";
	    
	    // sequence 받아오기
	    int seq = 1;
		try {
			sql = "Select max(SEQ) as M from " + tableName;
			dbconnection.runQuery(sql);
			dbconnection.getResult().next();
			seq = dbconnection.getResult().getInt("M") + 1;
		} catch (Exception e) {
			// TODO Auto-generated catch block
		}
		
	    // api data 받아서 csv파일 생성
	    String FileName = tableName + "_" + BaseDate + BaseTime + ".csv";
	    BufferedWriter bufWriter = new BufferedWriter(new FileWriter(FileName));
	    CreateCSV(bufWriter, column);
	    apiconnection = new ApiConnection();
	    int pagesize = 99999;
	    for(int count = 1; count < pagesize + 1; count++)
	    {
	    	try 
	    	{
	    		apiconnection.setUrl("http://data.ex.co.kr/exopenapi/vms/vmsMessageSrch");
				apiconnection.setServiceKey("serviceKey", "aq%2Bd7pEryGFmGFAAIFv8VQps%2FF5YNIGe4RZX%2F2SW4h1%2BGHoWs6c4M9QptIPsQPZ2yHhm5iBOnoKKS89LJtlDNA%3D%3D");
				apiconnection.urlAppender("numOfRows", "9999");
				apiconnection.urlAppenderNoTrans("type", "xml");
				apiconnection.urlAppender("pageNo", String.valueOf(count));
				apiconnection.pullData();
				System.out.println(apiconnection.urlBuilder);
				if(pagesize == 99999) pagesize = Integer.valueOf(apiconnection.result.select("pageSize").text());
				Elements elements = apiconnection.result.select("vmsMessageSrchLists");
				List<List<String>> data = new ArrayList<List<String>>();
				for(int i = 0; i < column.size(); i++)
				{
					data.add(new ArrayList<String>());
				}
				for(Element e : elements)
				{
					data.get(0).add(String.valueOf(seq));
					seq++;
					for(int i = 1; i < column.size(); i++)
					{
						data.get(i).add(e.select(column.get(i)).text());
					}
				}
				WriteCSV(bufWriter, data); 
			} catch (Exception e) 
	    	{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    bufWriter.close();
	    
	    // DB에 입력
	    sql = "LOAD DATA LOCAL INFILE '" + FileName + "' INTO TABLE " + tableName + " FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES";
	    dbconnection.LoadLocalData(sql);
	}
	
	public static void CreateCSV(BufferedWriter bufWriter, List<String> Column)
	{
		try
		{
			int i = 0;
			for(; i < Column.size() - 1; i++)
			{
				bufWriter.write("\"" + Column.get(i) + "\",");
			}
			bufWriter.write("\"" + Column.get(i) + "\"");
			bufWriter.newLine();
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void WriteCSV(BufferedWriter bufWriter, List<List<String>> datalist) throws IOException
	{
//		System.out.println(datalist.get(0).size() + " " + datalist.get(1).size()+ " " + datalist.get(2).size()+ " " + datalist.get(3).size()+ " " + datalist.get(4).size()+ " " + datalist.get(5).size()+ " " + datalist.get(6).size());
		String buffer = "";
		for(int i = 0; i < datalist.get(0).size(); i++)
		{
			int j = 0;
			for(; j < datalist.size() - 1; j++)
			{
				if(datalist.get(j).size() > i && datalist.get(j).get(i).contains("</"))
				{
					buffer += "\"" + datalist.get(j).get(i).substring(0,datalist.get(j).get(i).indexOf('<') ) + "\",";
				}
				else
				{
					buffer += "\"" + datalist.get(j).get(i) + "\",";
				}
			}
			if(datalist.get(j).size() > i && datalist.get(j).get(i).contains("</"))
			{
				buffer += "\"" + datalist.get(j).get(i).substring(0,datalist.get(j).get(i).indexOf('<') );
			}
			else
			{
				buffer += "\"" + datalist.get(j).get(i);
			}
			buffer += "\"\n";
		}
		System.out.print(buffer);
		bufWriter.write(buffer);
	}
}
