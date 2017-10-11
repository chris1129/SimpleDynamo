package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.util.Pair;

import static android.R.id.list;

public class SimpleDynamoProvider extends ContentProvider {
	private static final int SERVER_PORT = 10000;
	static final String TAG="SimpleDhtProvider";
	private String myIDhash;
	private String preIDhash=null;
	private  String succeIDhash=null;
	private  String myID;
	private  String preID=null;
	private  String succeID=null;
	private String centralnode=null;
	private String smallestnodehash;
	private  String largestnodehash;
	private ArrayList<String> nodelist;
	private  ArrayList<String> nodelist_originalID;
	private SQLdbHelper Mdbh;
	private SQL_my_temp_db Mtdb;
	private int flag=0;
	Uri uri=buildUri("content","edu.buffalo.cse.cse486586.simpledht");
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		if(selection.equals("@")){
			SQLiteDatabase db=Mdbh.getWritableDatabase();
			int aa=db.delete(SQLdbHelper.TABLE_NAME,null,null);
		}else if(selection.equals("*")){
			MessageObj msgobj=new MessageObj("delete_all",myID);
			msgobj.selection="@";
			if(nodelist_originalID.size()>0){
				Log.d(TAG,"Delete node list size="+nodelist_originalID.size());
				for(int i=0;i<nodelist_originalID.size();i++){
					msgobj.msg_desti_port=nodelist_originalID.get(i);
					sendmsg(msgobj);//client&Server
				}
			}else{
				Log.d(TAG,"Delete Task * :node size="+nodelist_originalID.size());
				SQLiteDatabase db=Mdbh.getWritableDatabase();
				db.delete(SQLdbHelper.TABLE_NAME,null,null);
			}
		}else{
			Log.d(TAG,"Delete key:"+selection);
			if((preIDhash==myIDhash&&succeIDhash==myIDhash)||(preIDhash==null&&succeIDhash==null)){
				Log.d(TAG,"Delete key Task: Only one node in ring");
				SQLiteDatabase db=Mdbh.getWritableDatabase();
				int aa=db.delete(SQLdbHelper.TABLE_NAME,"key=?",new String[]{selection});

			}else{
				Log.d(TAG,"Not only one node in ring");
				String hashselection=null;
				try {
					hashselection=genHash(selection);
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				if(hashselection.compareTo(nodelist.get(nodelist.size()-1))>0){

					Log.d(TAG,"Delete one key from the first node");
					MessageObj msgobj=new MessageObj("delete_key",myID);
					msgobj.selection=selection;
					msgobj.msg_desti_port=transhashid_to_orig(nodelist.get(0));
					sendmsg(msgobj);
				}else{
					for(int i=0;i<nodelist.size();i++){
						Log.d(TAG,"Delete Task: For loop to find the node has the key:"+nodelist.get(i));
						if(hashselection.compareTo(nodelist.get(i))<=0){
							MessageObj msgobj=new MessageObj("delete_key",myID);
							msgobj.selection=selection;
							msgobj.msg_desti_port=transhashid_to_orig(nodelist.get(i));
							sendmsg(msgobj);
						}
					}
				}

			}
		}

		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key=values.get("key").toString();
		String value=values.get("value").toString();
		String hashkey=null;


		try {
			hashkey=genHash(key);//hashkey used to find where to insert
			Log.d(TAG, "<Key,value>:"+key+","+value+"||hash="+hashkey);

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG,"hashKey=genHash wrong");
			e.printStackTrace();
		}
		if((preIDhash==myIDhash&&succeIDhash==myIDhash)||(preIDhash==null&&succeIDhash==null)){
			Log.d(TAG,"Only one node in ring");
			insert_to_mydb(values);


		}else{///////////////
			Log.d(TAG,"Not the only one in ring");
			/*if(hashkey.compareTo(myIDhash)<=0&&hashkey.compareTo(preIDhash)>0){
				insert_to_mydb(values);
				insert_to_next_twoSuc(values);
			}else{
				for(int i=0;i<nodelist.size();i++){
					if(hashkey.compareTo(nodelist.get(i))<=0){
						MessageObj msgobj=new MessageObj("require_insert",myID,key,value);
						msgobj.msg_desti_port=nodelist.get(i);
						sendmsg(msgobj);
					}

				}

			}*/
			if(smallestnodehash.compareTo(myIDhash)==0){// I am the smallest node
				Log.d(TAG,"I am the smallest node");
				if(hashkey.compareTo(myIDhash)<=0||hashkey.compareTo(largestnodehash)>0){
					Log.d(TAG,"Orignially in this node: "+values.get("Key")+","+values.get("value"));
					insert_to_mydb(values);
					insert_to_next_twoSuc(values);

				}else{
					Log.d(TAG,"pass to sucID:"+succeID);
					String tempmyid=transhashid_to_orig(myIDhash);
					passtosucID_for_insert(tempmyid,succeIDhash,values);
				}

			}else if(hashkey.compareTo(myIDhash)>0){//pass to sucID
				Log.d(TAG,"pass to sucID"+succeID);
				String tempmyid=transhashid_to_orig(myIDhash);
				passtosucID_for_insert(tempmyid,succeIDhash,values);

			}else if(hashkey.compareTo(myIDhash)<=0&&hashkey.compareTo(preIDhash)>0){//insert to my database
				Log.d(TAG,"Not only me in ring Insert to my database");
				Log.d(TAG,"Originially in this node: "+values.get("key")+","+values.get("value"));
				insert_to_mydb(values);
				insert_to_next_twoSuc(values);

			}else{//pass to preID
				Log.d(TAG,"pass to preID");
				String tempmyid=transhashid_to_orig(myIDhash);
				passtosucID_for_insert(tempmyid,preIDhash,values);


			}

		}

		return uri;
	}



	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myID=portStr;
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Log.d(TAG,"onCreat Task: portStr="+portStr);
		nodelist=new ArrayList<String>();
		nodelist_originalID=new ArrayList<String>();

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {

			Log.d(TAG, "Can't create a ServerSocket");
			;
		}


		try {
			myIDhash=genHash(portStr);
			Log.d(TAG,"portStr:"+portStr+"="+myIDhash);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		if(portStr.equals("5554")){
			Log.d(TAG,"Creat avd 5554");

			String message="requirejoin#"+portStr;
			MessageObj msgobj=new MessageObj("requirejoin",portStr);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgobj);
			Log.d(TAG,"Server Task: "+message);
		}else{         ////require to 5554 to add node

			String message="requirejoin#"+portStr;
			MessageObj msgobj=new MessageObj("requirejoin",portStr);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgobj);
			Log.d(TAG,"Server Task: "+message);


		}
		Mdbh=new SQLdbHelper(getContext());
		Mtdb=new SQL_my_temp_db(getContext());
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		SQLiteDatabase db=Mdbh.getReadableDatabase();
		SQLiteDatabase tdb=Mtdb.getReadableDatabase();
		Cursor cursor;
		Log.d(TAG,"Query: "+selection);

		String hashselection=null;
		try {
			hashselection=genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		if((preIDhash==myIDhash&&succeIDhash==myIDhash)||(preIDhash==null&&succeIDhash==null)){
			if(selection.equals("*")||selection.equals("@")){
				Log.d(TAG,"Query TAsk: Only one node in ring: Query * or @");

				cursor=db.query(SQLdbHelper.TABLE_NAME,null,null,null,null,null,null,null);
				return cursor;
			}else{
				Log.d(TAG,"Query TAsk: Only one node in ring: Query key:"+selection);

				cursor=db.query(SQLdbHelper.TABLE_NAME,null,"key = ? ",new String[] { selection},null,null,null,null);

				DatabaseUtils.dumpCursor(cursor);
				return cursor;
			}
		}else{
			if(selection.equals("@")){
				Log.d(TAG,"Selection is @");

				cursor=db.query(SQLdbHelper.TABLE_NAME,null,null,null,null,null,null,null);
				return cursor;
			}else if(selection.equals("*")){

				Log.d(TAG,"Query Task: Query * from"+myID);
				for(int i=0;i<nodelist_originalID.size();i++){

					query_all_from_other(nodelist_originalID.get(i));

				}
				while(flag!=nodelist.size()){

				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				flag=0;
				cursor=tdb.query(SQL_my_temp_db.Global_TABLE_NAME,null,null,null,null,null,null,null);
				DatabaseUtils.dumpCursor(cursor);

				return cursor;

			}else{
				Log.d(TAG,"Query key:"+selection);
				//if(hashselection.compareTo(myIDhash)<=0)
				if(hashselection.compareTo(nodelist.get(nodelist.size()-1))>0){
					Log.d(TAG,"Query from the last node");

					cursor=query_from_other_node(selection,nodelist.get(0));

					return cursor;
				}
				for(int i=0;i<nodelist.size();i++){
					Log.d(TAG,"For loop to find the node has the key:"+nodelist.get(i));

					if(hashselection.compareTo(nodelist.get(i))<=0){

						cursor=query_from_other_node(selection,nodelist.get(i));
						return cursor;

					}
				}


				//cursor=query_task();
				//cursor=db.query(SQLdbHelper.TABLE_NAME,null,"key = ? ",new String[] { selection},null,null,null,null);

				//DatabaseUtils.dumpCursor(cursor);


				//return cursor;
			}


		}
		// TODO Auto-generated method stub
		return null;
	}

	private void query_all_from_other(String targetid) {
		Log.d(TAG,"query_all_from: "+targetid);
		MessageObj msgobg=new MessageObj("query_local_all",myID);
		//msgobg.query_all_list=new ArrayList<Pair<String,String>>();
		msgobg.keyValueMap=new HashMap<String,String>();
		msgobg.msg_desti_port=targetid;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgobg);
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}



	private class ServerTask extends AsyncTask<ServerSocket, String, Void>{
		@Override
		protected Void doInBackground(ServerSocket... params) {
			ServerSocket serverSocket = params[0];

			try{
				while(true){

					Socket socket = serverSocket.accept();
					ObjectInputStream in=new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
					MessageObj msgobj=(MessageObj) in.readObject();
					// Log.d(TAG,"Server Task: Receive message: "+msgobj.messagetype+" "+msgobj.fromID);
					if(msgobj.messagetype.equals("requirejoin")){//assign preID, succID
						Log.d(TAG,"AVD 5554 Start to add new node"+msgobj.fromID+" join....");
						String newjoinID=genHash(msgobj.fromID);
						nodelist.add(newjoinID);
						nodelist_originalID.add(msgobj.fromID);
						Collections.sort(nodelist);
						Log.d(TAG,"Nodelist:"+nodelist);
						Log.d(TAG,"Original Node list: "+nodelist_originalID);

						msgobj.messagetype="newjoinlist";
						msgobj.nodelist=nodelist;
						msgobj.nodelist_originalID=nodelist_originalID;
						broadcast_new_list(msgobj);
						//msgobj contain new list and new original node list and fromID is the new join node



					}else if(msgobj.messagetype.equals("newjoinlist")){
						Log.d(TAG,"Server Task: Receive newjoinlist"+msgobj.nodelist);
						Log.d(TAG,"Server Task: Receive newjoinlist_orig"+msgobj.nodelist_originalID);
						nodelist_originalID=msgobj.nodelist_originalID;
						nodelist=msgobj.nodelist;
						smallestnodehash=nodelist.get(0);
						Log.d(TAG,"Update smallest node="+smallestnodehash);
						largestnodehash=nodelist.get(nodelist.size()-1);
						Log.d(TAG,"Update largest node="+largestnodehash);

						for(int i=0;i<msgobj.nodelist.size();i++){

							if(msgobj.nodelist.size()==1){
								Log.d(TAG,"Arrange Task: Only one node in list");
								preIDhash=myIDhash;
								succeIDhash=myIDhash;

							}else{
								Log.d(TAG,"notlonely myID="+myIDhash);
								if(msgobj.nodelist.get(i).equals(myIDhash) && i>0 && i<msgobj.nodelist.size()-1){
									Log.d(TAG,"Find myID pattern 1:");
									preIDhash=msgobj.nodelist.get(i-1);
									succeIDhash=msgobj.nodelist.get(i+1);


									break;

								}else if(msgobj.nodelist.get(i).equals(myIDhash) && i==0){
									Log.d(TAG,"Find myID pattern 2:");
									succeIDhash=msgobj.nodelist.get(i+1);
									//Log.d(TAG,"SUcceID: "+succeID);
									preIDhash=msgobj.nodelist.get(msgobj.nodelist.size()-1);
									//Log.d(TAG,"PreID: "+preID);

									break;

								}else if(msgobj.nodelist.get(i).equals(myIDhash) && i==msgobj.nodelist.size()-1){
									Log.d(TAG,"Find myID pattern 3:");
									succeIDhash=msgobj.nodelist.get(0);
									preIDhash=msgobj.nodelist.get(i-1);

									break;


								}


							}


						}
						Log.d(TAG,"ReArrange Task completed:"+myIDhash+" PreID="+preIDhash+" SucID="+succeIDhash);
						Log.d(TAG,"ReArrange Task completed:"+myID+" PreID="+preID+" SucID="+succeID);


					}else if(msgobj.messagetype.equals("require_insert")){
						Log.d(TAG,"Server Task: Receive require_insert");
						ContentValues ct=new ContentValues();
						ct.put("key",msgobj.insertkey);
						ct.put("value",msgobj.insertvalue);
						insert(uri,ct);

					}else if(msgobj.messagetype.equals("require_query")){
						Log.d(TAG,"Server Task: Receive require_query");

						Cursor queryresult;
						SQLiteDatabase db=Mdbh.getReadableDatabase();
						queryresult=db.query(SQLdbHelper.TABLE_NAME,null,"key = ? ",new String[] { msgobj.selection},null,null,null,null);
						DatabaseUtils.dumpCursor(queryresult);
						queryresult.moveToFirst();
						Log.d(TAG,"get the queryresult");
						msgobj.queryresult=queryresult.getString(1);
						Log.d(TAG,"get the value");
						msgobj.msg_desti_port=msgobj.fromID;
						msgobj.fromID=myID;
						msgobj.messagetype="query_reply";
						Log.d(TAG,"Server Task: reply query from:"+msgobj.fromID+" to "+msgobj.msg_desti_port);
						sendmsg(msgobj);

					}else if(msgobj.messagetype.equals("query_reply")){
						Log.d(TAG,"Server Task: Receive query_reply from: "+msgobj.fromID);
						String queryanswer=msgobj.queryresult;
						ContentValues cv=new ContentValues();
						cv.put("key", msgobj.selection);
						cv.put("value", queryanswer);
						insert_to_mytempdb(cv);
						////////////need to insert to my db


					}else if(msgobj.messagetype.equals("query_local_all")){

						Log.d(TAG,"Server Task: Receive query_local_all"+msgobj.fromID);
						Cursor cursor=query(uri, null, "@", null, null);
						if(cursor.getCount()>0) {
							cursor.moveToFirst();
							while (!cursor.isLast()) {
								//ContentValues temp=new ContentValues();
								Log.d(TAG, myID + " Cursor poping:" + cursor.getString(1));
								//temp.put("key",cursor.getString(0));
								//temp.put("value",cursor.getString(1));
								//Pair<String,String>p=new Pair<String, String>(cursor.getString(0), cursor.getString(1));
								//msgobj.query_all_list.add(p);

								msgobj.keyValueMap.put(cursor.getString(0), cursor.getString(1));

								cursor.moveToNext();
							}
							ContentValues temp = new ContentValues();
							Log.d(TAG, myID + " Cursor poping:" + cursor.getString(1));
							msgobj.keyValueMap.put(cursor.getString(0), cursor.getString(1));
						}else{Log.d(TAG,"cursor Count=0");}

						Log.d(TAG,"query_all_list.size()="+Integer.toString(msgobj.keyValueMap.size()));
						msgobj.msg_desti_port=msgobj.fromID;
						msgobj.fromID=myID;
						msgobj.messagetype="query_local_done";
						sendmsg(msgobj);


					}else if(msgobj.messagetype.equals("query_local_done")){
						Log.d(TAG,"Server Task: Receive query_local_done from: "+msgobj.msg_desti_port);

						insert_to_my_gtable(msgobj.keyValueMap);

					}else if(msgobj.messagetype.equals("delete_all")){
						int dlt=delete(uri,msgobj.selection,null);
					}else if(msgobj.messagetype.equals("delete_key")){
						SQLiteDatabase db=Mdbh.getWritableDatabase();
						int aa=db.delete(SQLdbHelper.TABLE_NAME,"key=?",new String[]{msgobj.selection});


					}else if(msgobj.messagetype.equals("force_insert")){
						Log.d(TAG,"Server Task: Recevice force insert to my db:"+msgobj.insertkey);
						ContentValues cv=new ContentValues();
						cv.put("key", msgobj.insertkey);
						cv.put("value",msgobj.insertvalue);

						insert_to_mydb(cv);


					}

				}

			}catch (Exception e){

			}
			return null;
		}
	}

	private void insert_to_my_gtable(HashMap<String,String> map) {
		Log.d(TAG,"Insert_to_my_gtable:");
		Iterator<String> keySetIterator = map.keySet().iterator();
		while(keySetIterator.hasNext()){
			String key = keySetIterator.next();
			ContentValues cv=new ContentValues();
			cv.put("key",key);
			cv.put("value",map.get(key));
			Log.d(TAG,"Insert_to_gtable:<key,value>="+cv.get("key")+"||"+cv.get("value"));

			String test=((String)cv.get("key"));
			SQLiteDatabase db = Mtdb.getWritableDatabase();

			Cursor cursor=db.rawQuery("SELECT * FROM "+SQL_my_temp_db.Global_TABLE_NAME+" WHERE key = ? ",new String[] { test});
			if(cursor.getCount()>0){
				db.update(SQL_my_temp_db.Global_TABLE_NAME,cv,"key =" +
						" ?",new String[]{test});
				Log.v("rawQuery:","cursor!=null");
			}else{
				Log.v("rawQuery:","cursor==null");
				db.insert(SQL_my_temp_db.Global_TABLE_NAME,null,cv);

			}

		}
		flag++;
		Log.d(TAG,"insert_to_my_gtable: #flag="+flag);
	}

	private class  ClientTask extends AsyncTask<MessageObj,Void,Void>{

		@Override
		protected Void doInBackground(MessageObj... params) {
			Log.d(TAG,"Prepare to send: "+params[0].messagetype+params[0].msg_desti_port);
			MessageObj msgobj=params[0];
			try{

				if(msgobj.messagetype.equals("requirejoin")){
					Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt("11108"));

					Log.d(TAG,"CLIENT Send messgae to join: "+msgobj.messagetype+"/"+msgobj.fromID);

					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgobj);
					Log.d(TAG,"Client Sented");
				}else if(msgobj.messagetype.equals("newjoinlist")){
					for(int i=0;i<nodelist_originalID.size();i++){
						Log.d(TAG,"Client Task: broadcast new list to "+nodelist_originalID.get(i));
						String remote_port=Integer.toString(Integer.parseInt(nodelist_originalID.get(i))*2);////////
						Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(remote_port));

						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						out.writeObject(msgobj);

					}

				}else if(msgobj.messagetype.equals("sendmsg_to_insert")){
					Log.d(TAG,"Client Task: sendmsg_to_insert to:"+msgobj.msg_desti_port);
					msgobj.messagetype="require_insert";
					String to_port=msgobj.msg_desti_port;
					Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(to_port)*2);

					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgobj);


				}else if(msgobj.messagetype.equals("require_query")){
					Log.d(TAG,"Client Task: require_query to:"+msgobj.msg_desti_port);
					String to_port=msgobj.msg_desti_port;
					Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(to_port)*2);

					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgobj);

				}else if(msgobj.messagetype.equals("query_reply")){

					Log.d(TAG,"Client Task: send back query_reply to:"+msgobj.msg_desti_port);
					String to_port=msgobj.msg_desti_port;
					Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(to_port)*2);

					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgobj);
				}else if(msgobj.messagetype.equals("query_local_all")){
					Log.d(TAG,"Client Task: send query_local_all to:"+msgobj.msg_desti_port);
					String to_port=msgobj.msg_desti_port;
					Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(to_port)*2);

					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgobj);

				}else if(msgobj.messagetype.equals("query_local_done")){
					Log.d(TAG,"Client Task: send query_local_done to:"+msgobj.msg_desti_port);
					String to_port=msgobj.msg_desti_port;
					Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(to_port)*2);

					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgobj);


				}else{
					Log.d(TAG,"Client Task: send "+msgobj.messagetype+" to:"+msgobj.msg_desti_port);
					String to_port=msgobj.msg_desti_port;
					Socket socket=new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(to_port)*2);

					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(msgobj);
				}



			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}
	public void broadcast_new_list(MessageObj msgobj){
		Log.d(TAG,"Broadcast Task....");
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgobj);


	}
	public void sendmsg(MessageObj msgobj){
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgobj);

	}
	public void passtosucID_for_insert(String fromid,String toIDhash,ContentValues values){
		Log.d(TAG,"passtosucID Task...");
		MessageObj msgobj=new MessageObj("sendmsg_to_insert",fromid);
		String remoteport=transhashid_to_orig(toIDhash);
		Log.d(TAG,"passtosucID:Pass from "+fromid+" to "+remoteport);
		msgobj.msg_desti_port=remoteport;
		msgobj.insertkey=(String)values.get("key");
		msgobj.insertvalue=(String)values.get("value");

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgobj);

	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	public String transhashid_to_orig(String hashid){
		for(int i=0;i<nodelist_originalID.size();i++){//local nodelist orig
			String temp="";
			try {
				temp=genHash(nodelist_originalID.get(i));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			if(temp.equals(hashid)){
				Log.d(TAG,"transhashid_to_orig: Successful");
				return nodelist_originalID.get(i);
			}

		}
		Log.d(TAG,"transhashid_to_orig: Can't find"+hashid);
		return null;

	}
	public void insert_to_mytempdb(ContentValues values){
		Log.d(TAG,"Insert_to_Mytempdb:"+values.get("key").toString()+"||"+values.get("value").toString());

		String test=(values.get("key").toString());
		SQLiteDatabase tempdb = Mtdb.getWritableDatabase();

		Cursor cursor=tempdb.rawQuery("SELECT * FROM "+SQL_my_temp_db.TABLE_NAME+" WHERE key = ? ",new String[] { test});
		if(cursor.getCount()>0){
			tempdb.update(SQL_my_temp_db.TABLE_NAME,values,"key =" +
					" ?",new String[]{test});
			Log.v("rawQuery:","cursor!=null");
		}else{
			Log.v("rawQuery:","cursor==null");
			tempdb.insert(SQL_my_temp_db.TABLE_NAME,null,values);

		}
	}
	private void insert_to_next_twoSuc(ContentValues values) {
		//Log.d(TAG,"Insert to next two node Task:");
		int pos=0;
		int next=0;
		int nextnext=0;
		for(int i=0;i<nodelist_originalID.size();i++){
			if(nodelist_originalID.get(i).equals(myID))pos=i;

		}
		if(pos==nodelist.size()-1){
			next=0;
			nextnext=1;
		}else{
			next=pos+1;
			nextnext=pos+2;
		}
		Log.d(TAG,"Insert to next two node Task: myID="+myID+"next1:"+nodelist_originalID.get(next)+" next2:"+nodelist_originalID.get(nextnext));
		MessageObj msgobj=new MessageObj("force_insert",myID,(String)values.get("key"),(String)values.get("value"));
		msgobj.msg_desti_port=nodelist_originalID.get(next);
		sendmsg(msgobj);
		msgobj.msg_desti_port=nodelist_originalID.get(nextnext);
		sendmsg(msgobj);



	}

	public void insert_to_mydb(ContentValues values){
		Log.d(TAG,"Insert_to_mydb:"+values.get("key").toString()+"||"+values.get("value").toString());

		String test=(values.get("key").toString());
		SQLiteDatabase db = Mdbh.getWritableDatabase();

		Cursor cursor=db.rawQuery("SELECT * FROM "+SQLdbHelper.TABLE_NAME+" WHERE key = ? ",new String[] { test});
		if(cursor.getCount()>0){
			db.update(SQLdbHelper.TABLE_NAME,values,"key =" +
					" ?",new String[]{test});
			Log.v("rawQuery:","cursor!=null");
		}else{
			Log.v("rawQuery:","cursor==null");
			db.insert(SQLdbHelper.TABLE_NAME,null,values);

		}
	}
	public  Cursor query_from_other_node(String selection,String targetnodehash){
		Cursor cursor;
		Log.d(TAG,"Query_from_other_node TASK:");
		MessageObj msgquery=new MessageObj("require_query",myID);
		msgquery.selection=selection;
		msgquery.msg_desti_port=transhashid_to_orig(targetnodehash);
		//msgquery.querykey=selection;
		Log.d(TAG,myID+"require_query from"+msgquery.msg_desti_port);

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgquery);
		///////need to return the cursor by find the value receiving from server
		//cursor=db.query(SQL_my_temp_db.TABLE_NAME,null,null,null,null,null,null,null);

		SQLiteDatabase tempdb=Mtdb.getReadableDatabase();
		//cursor=tempdb.query(SQL_my_temp_db.TABLE_NAME,null,null,null,null,null,null,null);;
		cursor=tempdb.query(SQL_my_temp_db.TABLE_NAME,null,"key = ? ",new String[] { selection},null,null,null,null);
		Log.d(TAG,"cursor count= "+Integer.toString(cursor.getCount()));
		DatabaseUtils.dumpCursor(cursor);
		while(cursor.getCount()==0){
			//Log.d(TAG,"while loop");
			cursor=tempdb.query(SQL_my_temp_db.TABLE_NAME,null,"key = ? ",new String[] { selection},null,null,null,null);
		};
		return cursor;
	}


}
