package com.njp.learn.lucene.es1;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class FileUtil {
	private static final Gson gson = new Gson();

	public static String readJsonDefn(String url)  {
		// implement it the way you like
		StringBuffer bufferJSON = new StringBuffer();

		FileInputStream input;
		try {
			input = new FileInputStream(new File(url).getAbsolutePath());
			DataInputStream inputStream = new DataInputStream(input);
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

			String line;

			while ((line = br.readLine()) != null) {
				bufferJSON.append(line);
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}catch(IOException e){
			e.printStackTrace();
			return null;
		}

		if(isJSONValid(bufferJSON.toString())){
			return bufferJSON.toString();
		}
		
		return null;
	}

	public static boolean isJSONValid(String json) {
		JsonParser parser = new JsonParser();
		JsonElement  elem = parser.parse(json);
		System.out.println(elem.toString());
		
		if(elem.isJsonNull()){
			return false;
		}else{
			return true;
		}
	}
	
	public static JsonObject getJson(String file){
		// implement it the way you like
				StringBuffer bufferJSON = new StringBuffer();
				JsonObject object = null;
				FileInputStream input;
				try {
					input = new FileInputStream(new File(file).getAbsolutePath());
					DataInputStream inputStream = new DataInputStream(input);
					BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

					JsonParser parser = new JsonParser();
					JsonElement  elem = parser.parse(br);
					System.out.println(elem.toString());
					
					object  = elem.getAsJsonObject();
					
					br.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}catch(IOException e){
					e.printStackTrace();
					return null;
				}


				
				return object;
	}
	
	public static void main(String[] args){
		String filename="src/main/resources/conf/db.json";
		String content = readJsonDefn(filename);
		JsonObject object = getJson(filename);
		
		String JDBC_DRIVER = object.get("jdbc").getAsString();
		String DB_URL = object.get("url").getAsString();
		String USER = object.get("username").getAsString();
		String PASS = object.get("password").getAsString();
		
		System.out.println(JDBC_DRIVER);
		System.out.println(DB_URL);
		System.out.println(USER);
		System.out.println(PASS);
		
		
		HashMap<String, String> schema = new HashMap<String,String>();
		
		object.get("schema").getAsJsonArray();
		
		for(JsonElement elem : object.get("schema").getAsJsonArray() ){
			schema.put(elem.getAsJsonObject().get("name").getAsString(), elem.getAsJsonObject().get("type").getAsString());
		}
		
		for(String item : schema.keySet()){
			System.out.println("key : " + item + " ==> value : " + schema.get(item));
		}
		
		System.out.println(content.isEmpty());

		
	}
}
