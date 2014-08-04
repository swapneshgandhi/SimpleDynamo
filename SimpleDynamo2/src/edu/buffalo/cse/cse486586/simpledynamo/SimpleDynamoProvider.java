package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	private SQLLiteDBHelper database;
	SQLiteDatabase sqlDB;
	String TAG="provider";
	static final int SERVER_PORT = 10000;
	public String remotePort []= {"11108","11112","11116","11120","11124"};
	public static String myPort; 
	private CirLinkedList NodeList; 
	private Senders sender;
	public static HashMap <String,String []>ReplyMap;
	public static HashMap <String,ArrayList<ReplyArray>>QueryMap;
	public static HashMap <String,ArrayList<ReplyArray>>FailureMap;
	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock read  = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();

	public void Peer_Delete(Message message){
		try{write.lock();
		sqlDB.delete(SQLLiteDBHelper.TABLE_NAME, SQLLiteDBHelper.Key_Column + "=?",new String [] {message.Values[0]});
		sender.Log_dis("Deleted ",message.Values[0]);}
		finally{write.unlock();}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		String [] vals = new String [3];
		vals[0]=selection;

		String hashVal="";

		try {
			hashVal = genHash(selection);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String Nodes[]= new String [3];
		Nodes=NodeList.getConcernedNodes(hashVal);
		Message message=new Message("Delete",myPort,vals,Nodes);
		sender.SendTask(message);
		return 1;
	}

	@Override
	public String getType(Uri uri) {

		return null;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		String [] vals = new String [3];
		vals[0]=selection;

		String hashVal="";

		if(selection.equals("@")){
			String [] Nodes=new String [3];
			Nodes[0]=myPort;
			Nodes[1]=NodeList.getNext(NodeList.getPosition(myPort)).Port;
			Nodes[2]=NodeList.getNexttoNext(NodeList.getPosition(myPort)).Port;
			Message message=new Message("Query",myPort,vals,Nodes);
			sender.SendTask(message);
			Cursor Cur=null;
			Log.e("query@","@");
			try {
				Cur = waitQueryMap(selection);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return Cur;
		}
		else if(selection.equals("*")){
			String [] Nodes=new String [5];
			Nodes[0]=myPort;
			Nodes[1]=NodeList.getNext(NodeList.getPosition(myPort)).Port;
			Nodes[2]=NodeList.getNexttoNext(NodeList.getPosition(myPort)).Port;
			Nodes[3]=NodeList.getPrev(NodeList.getPosition(myPort)).Port;
			Nodes[4]=NodeList.getPrevtoPrev(NodeList.getPosition(myPort)).Port;			
			Message message=new Message("Query",myPort,vals,Nodes);
			Log.e("Query for ",selection);
			sender.SendTask(message);
			Cursor Cur=null;
			try {
				Cur = waitQueryMap(selection);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return Cur;
		}

		else{		
			try {
				hashVal = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String [] Nodes=new String [3];
			Nodes=NodeList.getConcernedNodes(hashVal);
			Message message=new Message("Query",myPort,vals,Nodes);			
			Log.e("Query:Concerned node for "+" "+message.ConcernedNodes[0]+" "+message.ConcernedNodes[1]+" "+message.ConcernedNodes[2],selection);
			sender.SendTask(message);
			Cursor Cur=null;
			try {
				Cur = waitQueryMap(selection);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return Cur;
		}
	}

	public Cursor waitQueryMap(String selection) throws InterruptedException{
		String[] columns = new String[] {
				SQLLiteDBHelper.Key_Column,
				SQLLiteDBHelper.Value_Column
		};
		int timeout=600;
		if (selection.contains("*") || selection.contains("@")){
			timeout=1650;
		}
		MatrixCursor  Cur = new MatrixCursor(columns);


		int i=0;
		Thread.sleep(timeout);
		i=0;
		Log.e("query wait over", selection);
		ArrayList <ReplyArray> RepArray = QueryMap.get(selection);
		while(RepArray==null){
			Log.e("sitting duck","here");
			Thread.sleep(timeout/2);
			RepArray = QueryMap.get(selection);
		}
		while (i<RepArray.size()){
			Log.e("Q:",RepArray.get(i).Key+" "+RepArray.get(i).Value);
			Cur.addRow(new Object[] {RepArray.get(i).Key,RepArray.get(i).Value});
			i++;
		}
		QueryMap.remove(selection);
		Cur.moveToFirst();
		return Cur;
	}

	public void failQueryAllData(Message message){
		int keyIndex;
		int valueIndex;
		int versIndex;
		Cursor cur;

		try {read.lock();
		cur = sqlDB.query(SQLLiteDBHelper.TABLE_NAME, null, null,null,null,null,null);}
		finally{read.unlock();}

		ArrayList<ReplyArray> reply = new ArrayList <ReplyArray>();
		cur.moveToFirst();
		keyIndex = cur.getColumnIndex(SQLLiteDBHelper.Key_Column);
		valueIndex = cur.getColumnIndex(SQLLiteDBHelper.Value_Column);
		versIndex = cur.getColumnIndex(SQLLiteDBHelper.Vers_Column);

		while (cur.isAfterLast() == false) 
		{
			ReplyArray rep=new ReplyArray();
			rep.Key  = cur.getString(keyIndex);
			rep.Value  = cur.getString(valueIndex);
			rep.Version= cur.getString(versIndex);

			String msgIniHash=NodeList.get(NodeList.getPosition(message.MsgInitiator)).hash;
			String msgprevHash=NodeList.getPrev(NodeList.getPosition(message.MsgInitiator)).hash;

			try {
				if((message.Values[0].equals("@*") && genHash(rep.Key).compareTo(msgprevHash)>0 && 
						genHash(rep.Key).compareTo(msgIniHash)<0) ||
						(message.Values[0].equals("@*") && NodeList.getPosition(message.MsgInitiator)==0 &&
						(genHash(rep.Key).compareTo(msgIniHash)<0 || genHash(rep.Key).compareTo(msgprevHash)>0))){ 
					Log.e("FailQuery "+message.Values[0]+" "+message.MsgInitiator ,rep.Key+" "+rep.Value);
					reply.add(rep);
				}

			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			cur.moveToNext();
		}

		String ConcernedNodes[]=new String [1]; 
		ConcernedNodes[0]=message.MsgInitiator;
		Message repmsg=new Message("FailureReply",myPort,message.Values,ConcernedNodes);	
		repmsg.Reply=reply;
		sender.reply_to_Query(repmsg);

	}

	public void failQueryOnlyMyData(Message message){
		int keyIndex;
		int valueIndex;
		int versIndex;
		Cursor cur;

		try {read.lock();
		cur = sqlDB.query(SQLLiteDBHelper.TABLE_NAME, null, null,null,null,null,null);}
		finally{read.unlock();}

		ArrayList<ReplyArray> reply = new ArrayList <ReplyArray>();
		cur.moveToFirst();
		keyIndex = cur.getColumnIndex(SQLLiteDBHelper.Key_Column);
		valueIndex = cur.getColumnIndex(SQLLiteDBHelper.Value_Column);
		versIndex = cur.getColumnIndex(SQLLiteDBHelper.Vers_Column);

		while (cur.isAfterLast() == false) 
		{
			ReplyArray rep=new ReplyArray();
			rep.Key  = cur.getString(keyIndex);
			rep.Value  = cur.getString(valueIndex);
			rep.Version= cur.getString(versIndex);

			String msgIniHash=NodeList.get(NodeList.getPosition(myPort)).hash;
			String msgprevHash=NodeList.getPrev(NodeList.getPosition(myPort)).hash;

			try {
				if((message.Values[0].equals("@@") && genHash(rep.Key).compareTo(msgprevHash)>0 && 
						genHash(rep.Key).compareTo(msgIniHash)<0) ||
						(message.Values[0].equals("@@") && NodeList.getPosition(myPort)==0 &&
						(genHash(rep.Key).compareTo(msgIniHash)<0 || genHash(rep.Key).compareTo(msgprevHash)>0))){ 
					Log.e("FailQuery "+message.Values[0]+" "+message.MsgInitiator+":"+myPort ,rep.Key+" "+rep.Value);
					reply.add(rep);
				}

			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			cur.moveToNext();
		}

		String ConcernedNodes[]=new String [1]; 
		ConcernedNodes[0]=message.MsgInitiator;
		Message repmsg=new Message("FailureReply",myPort,message.Values,ConcernedNodes);	
		repmsg.Reply=reply;
		sender.reply_to_Query(repmsg);
	}

	public void Peer_Query(Message message) {
		int keyIndex;
		int valueIndex;
		int versIndex;
		Cursor cur=null;

		Log.e("query from "+message.MsgInitiator,message.Values[0] );

		if(message.Values[0].equals("@") || message.Values[0].equals("*")){
			try {read.lock();
			cur = sqlDB.query(SQLLiteDBHelper.TABLE_NAME, null, null,null,null,null,null);}
			finally{read.unlock();}
		}

		else if(message.Values[0].equals("@*")){
			failQueryAllData(message);
			return;
		}

		else if(message.Values[0].equals("@@")){
			failQueryOnlyMyData(message);
			return;
		}

		else{
			try {read.lock();
			cur = sqlDB.query(SQLLiteDBHelper.TABLE_NAME,null, SQLLiteDBHelper.Key_Column + "=?",new String [] {message.Values[0]},null,null,null);}		
			finally{read.unlock();}
		}

		ArrayList<ReplyArray> reply = new ArrayList <ReplyArray>();
		cur.moveToFirst();
		keyIndex = cur.getColumnIndex(SQLLiteDBHelper.Key_Column);
		valueIndex = cur.getColumnIndex(SQLLiteDBHelper.Value_Column);
		versIndex = cur.getColumnIndex(SQLLiteDBHelper.Vers_Column);

		while (cur.isAfterLast() == false) 
		{
			ReplyArray rep=new ReplyArray();
			rep.Key  = cur.getString(keyIndex);
			rep.Value  = cur.getString(valueIndex);
			rep.Version= cur.getString(versIndex);

			String msgIniHash=NodeList.get(NodeList.getPosition(message.MsgInitiator)).hash;
			String msgprevHash=NodeList.getPrev(NodeList.getPosition(message.MsgInitiator)).hash;

			try {
				if(!message.Values[0].equals("@") || 
						(message.Values[0].equals("@") && genHash(rep.Key).compareTo(msgprevHash)>0 && 
								genHash(rep.Key).compareTo(msgIniHash)<0) ||
								(message.Values[0].equals("@") && NodeList.getPosition(message.MsgInitiator)==0 &&
								(genHash(rep.Key).compareTo(msgIniHash)<0 || genHash(rep.Key).compareTo(msgprevHash)>0)) ||
								(message.MsgInitiator.equals(myPort) && message.Values[0].equals("@"))){ 

					reply.add(rep);
				}
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			cur.moveToNext();
		}

		String ConcernedNodes[]=new String [1]; 
		ConcernedNodes[0]=message.MsgInitiator;
		Message repmsg=new Message("QueryReply",myPort,message.Values,ConcernedNodes);	
		repmsg.Reply=reply;
		sender.reply_to_Query(repmsg);
	}

	public Cursor QuerytheDB(String Selection){

		String [] SelectionArgs = {Selection};

		if ( SelectionArgs[0].equals("@") || SelectionArgs[0].equals("*")){
			Cursor cursor =	sqlDB.query(SQLLiteDBHelper.TABLE_NAME, null, null,null,null,null,null);
			int keyIndex = cursor.getColumnIndex("key");
			int valueIndex = cursor.getColumnIndex("value");

			cursor.moveToFirst();

			while (cursor.isAfterLast() == false){
				String returnKey = cursor.getString(keyIndex);
				String returnValue = cursor.getString(valueIndex);

				sender.Log_dis(returnKey +"-"+returnValue,"-\n");
				cursor.moveToNext();
			}
			return cursor;
		}

		Cursor cursor = sqlDB.query(SQLLiteDBHelper.TABLE_NAME, null, SQLLiteDBHelper.Key_Column + "=?",SelectionArgs,null,null,null);
		//Log.e(cursor.getString(1),cursor.getString(2));
		int keyIndex = cursor.getColumnIndex("key");
		int valueIndex = cursor.getColumnIndex("value");

		cursor.moveToFirst();

		while (cursor.isAfterLast() == false){
			String returnKey = cursor.getString(keyIndex);
			String returnValue = cursor.getString(valueIndex);

			sender.Log_dis(returnKey +"-"+returnValue,"-");
			cursor.moveToNext();
		}
		return cursor;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String returnVersion="0";

		String Key=(String) values.get(SQLLiteDBHelper.Key_Column);
		String [] vals = new String [3];
		vals[0]=(String)values.get(SQLLiteDBHelper.Key_Column);
		vals[1]=(String)values.get(SQLLiteDBHelper.Value_Column);

		String hashVal="";

		try {
			hashVal = genHash(Key);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Message message=new Message("Insert",myPort,vals,NodeList.getConcernedNodes(hashVal));
		Log.e("Insert:Concerned node for"+" "+message.ConcernedNodes[0]+" "+message.ConcernedNodes[1]+" "+message.ConcernedNodes[2],vals[0]);
		sender.SendTask(message);

		try {
			Thread.sleep(300);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String [] Retvalues=ReplyMap.get(vals[0]);
		if(Retvalues!=null){
			Log.e("insertwaitoverfor",vals[0]+" "+ Retvalues[2]);
		}
		if(Retvalues==null || Retvalues[2].compareTo(returnVersion) < 0 ){
			message.Values[2]=String.valueOf(Integer.parseInt(returnVersion)+1);

		}
		else{
			message.Values[2]=String.valueOf(Integer.parseInt(Retvalues[2])+1);
		}
		ReplyMap.remove(vals[0]);

		message.MessageType="ReplicateInsert";
		sender.SendTask(message);
		return uri;
	}

	public void replicateInsert(Message message){
		ContentValues vals = new ContentValues();
		vals.put("key",message.Values[0]);
		vals.put("value",message.Values[1]);
		vals.put("version",message.Values[2]);
		try{write.lock();
		sqlDB.insertWithOnConflict(SQLLiteDBHelper.TABLE_NAME, null, vals, SQLiteDatabase.CONFLICT_REPLACE);
		sender.Log_dis("InsertedR " ,message.Values[0]+" "+message.Values[1]);}
		finally{write.unlock();}
	}
	public void Peer_insert(Uri uri, Message message) {
		int keyIndex;
		int valueIndex;
		int versIndex;
		String returnKey="0";
		String returnValue="0";
		String returnVersion="0";
		Cursor cursor;
		try {read.lock();
		cursor = sqlDB.query(SQLLiteDBHelper.TABLE_NAME, null, SQLLiteDBHelper.Key_Column + "=?",new String [] {message.Values[0]},null,null,null);}		
		finally{read.unlock();}
		if(cursor!=null && cursor.moveToFirst()){
			keyIndex = cursor.getColumnIndex("key");
			valueIndex = cursor.getColumnIndex("value");
			versIndex= cursor.getColumnIndex("version");
			//get latest version
			returnKey = cursor.getString(keyIndex);
			returnValue = cursor.getString(valueIndex);
			returnVersion = cursor.getString(versIndex);
		}	

		String [] returnValues=null;
		if(!returnKey.equals("0")){
			returnValues= new String [3];
			returnValues[0]=returnKey;
			returnValues[1]=returnValue;
			returnValues[2]=returnVersion;
		}
		String [] ConcernedNodes=new String[1];
		ConcernedNodes[0]=message.MsgInitiator;

		Message repmsg = new Message("WriteQ",myPort,returnValues, ConcernedNodes);
		sender.Log_dis("Peer_ins",message.Values[0]);
		sender.SendTask(repmsg);
	}

	void waitFailureMap(){
		int i=0;
		int keyIndex;
		int valueIndex;
		int versIndex;
		String returnKey="0";
		String returnValue="0";
		String returnVersion="0";

		Cursor cursor;

		try {

			Thread.sleep(1650);
			i=0;
			Log.e("failure map wait over", "@*");
			ArrayList <ReplyArray> RepArray = FailureMap.get("@*");

			if(RepArray==null || RepArray.size()<1){		
				return;
			}

			i=0;
			while (i<RepArray.size()){
				returnVersion="0";
				try {read.lock();
				cursor = sqlDB.query(SQLLiteDBHelper.TABLE_NAME, null, SQLLiteDBHelper.Key_Column + "=?",new String [] {RepArray.get(i).Key},null,null,null);}		
				finally{read.unlock();}

				if(cursor!=null && cursor.moveToFirst()){
					keyIndex = cursor.getColumnIndex("key");
					valueIndex = cursor.getColumnIndex("value");
					versIndex= cursor.getColumnIndex("version");
					//get latest version
					returnKey = cursor.getString(keyIndex);
					returnValue = cursor.getString(valueIndex);
					returnVersion = cursor.getString(versIndex);
				}

				//insert only if key is not present or newer version
				if (returnVersion.equals("0") || returnVersion.compareTo(RepArray.get(i).Version)<=0){ 					
					ContentValues vals = new ContentValues();
					vals.put("key",RepArray.get(i).Key);
					vals.put("value",RepArray.get(i).Value);
					vals.put("version",RepArray.get(i).Version);

					try{write.lock();
					sqlDB.insertWithOnConflict(SQLLiteDBHelper.TABLE_NAME, null, vals, SQLiteDatabase.CONFLICT_REPLACE);
					sender.Log_dis("Inserted@* " , RepArray.get(i).Key+" "+RepArray.get(i).Value);}
					finally{write.unlock();}
				}
				else{
					Log.e("rejected@*",RepArray.get(i).Key+" NV"+RepArray.get(i).Version +" "+returnKey+" OV"+returnVersion);
				}
				i++;
			}
			FailureMap.remove("@*");
			Log.e("failure map wait over", "@@");

			RepArray = FailureMap.get("@@");
			i=0;
			while (i<RepArray.size()){
				returnVersion="0";
				try {read.lock();
				cursor = sqlDB.query(SQLLiteDBHelper.TABLE_NAME, null, SQLLiteDBHelper.Key_Column + "=?",new String [] {RepArray.get(i).Key},null,null,null);}		
				finally{read.unlock();}
				if(cursor!=null && cursor.moveToFirst()){
					keyIndex = cursor.getColumnIndex("key");
					valueIndex = cursor.getColumnIndex("value");
					versIndex= cursor.getColumnIndex("version");
					//get latest version
					returnKey = cursor.getString(keyIndex);
					returnValue = cursor.getString(valueIndex);
					returnVersion = cursor.getString(versIndex);
				}	
				if (returnVersion.equals("0") || returnVersion.compareTo(RepArray.get(i).Version)<=0){ 					
					ContentValues vals = new ContentValues();
					vals.put("key",RepArray.get(i).Key);
					vals.put("value",RepArray.get(i).Value);
					vals.put("version",RepArray.get(i).Version);

					try{write.lock();
					sqlDB.insertWithOnConflict(SQLLiteDBHelper.TABLE_NAME, null, vals, SQLiteDatabase.CONFLICT_REPLACE);
					sender.Log_dis("Inserted@@ " , RepArray.get(i).Key+" "+RepArray.get(i).Value);}
					finally{write.unlock();}
				}
				else{
					Log.e("rejected@@",RepArray.get(i).Key+" NV"+RepArray.get(i).Version +" "+returnKey+" OV"+returnVersion);
				}
				i++;
			}
			FailureMap.remove("@@");
		}
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean onCreate() {
		database = new SQLLiteDBHelper(getContext());
		sqlDB=database.getWritableDatabase();
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		sender= new Senders();
		NodeList= new CirLinkedList(); 
		ReplyMap = new HashMap<String,String[]>();
		QueryMap = new HashMap<String,ArrayList<ReplyArray>>();
		FailureMap = new HashMap<String,ArrayList<ReplyArray>>();
		try {
			NodeList.createList();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		CreateServerSocket();
		//failure handling ask for data
		String [] vals1 = new String [3];
		vals1[0]="@*";
		String Nodes1[] = new String [2];
		Nodes1[0]=NodeList.getNext(NodeList.getPosition(myPort)).Port;
		Nodes1[1]=NodeList.getNexttoNext(NodeList.getPosition(myPort)).Port;
		Message message=new Message("Query",myPort,vals1,Nodes1);
		sender.reply_to_Query(message);

		String [] vals2 = new String [3];
		vals2[0]="@@";
		String Nodes2[] = new String [2];
		Nodes2[0]=NodeList.getPrev(NodeList.getPosition(myPort)).Port;
		Nodes2[1]=NodeList.getPrevtoPrev(NodeList.getPosition(myPort)).Port;
		message=new Message("Query",myPort,vals2,Nodes2);
		sender.reply_to_Query(message);
		waitFailureMap();
		Log.e("Create","done");
		return false;
	}


	public void CreateServerSocket(){

		try {
			ServerSocket serverSocket = new ServerSocket(10000);
			new ServerTask().StartServer(serverSocket);	
		}

		catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return;
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {

		return 0;
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	void HandleQuorumReply(String reply[]){

		String [] presentRep = new String [3];
		if (reply!=null){
			if(ReplyMap.get(reply[0])!=null){
				presentRep=ReplyMap.get(reply[0]);
				if(presentRep[2].compareTo(reply[2])<0){
					ReplyMap.put(reply[0],reply);
				}
			}
			else{
				ReplyMap.put(reply[0],reply);
			}
		}
	}

	void HandleQueryReply(Message message){
		Log.e("QMap "+message.MsgInitiator+" ",message.Values[0]);

		ArrayList <ReplyArray> oldentries= new ArrayList <ReplyArray>();

		if(QueryMap.get(message.Values[0])==null){
			QueryMap.put(message.Values[0], message.Reply);			
		}

		else{
			int i=0;
			int flag=0;
			oldentries=QueryMap.get(message.Values[0]);

			while(i<message.Reply.size()){
				int j=0;

				while(j<oldentries.size()){

					if(oldentries.get(j).Key.equals(message.Reply.get(i).Key)){

						flag=1;
						if(oldentries.get(j).Version.compareTo(message.Reply.get(i).Version)<0){
							oldentries.set(j,message.Reply.get(i));

						}
						Log.e(message.Reply.get(i).Key,"flag="+flag+"V"+message.Reply.get(i).Version+" "+oldentries.get(j).Version);
						break;
					}
					j++;
				}		

				if(flag!=1){
					oldentries.add(message.Reply.get(i));
				}

				i++;
				flag=0;
			}
			QueryMap.put(message.Values[0], oldentries);
		}
		Log.e("gave up the lock",message.Values[0]);
	}

	void HandleFailureReply(Message message){
		Log.e("FailMap "+message.MsgInitiator+" ",message.Values[0]);
		if(message.Reply.size()<1){
			return;
		}

		ArrayList <ReplyArray> oldentries= new ArrayList <ReplyArray>();

		if(FailureMap.get(message.Values[0])==null){
			FailureMap.put(message.Values[0], message.Reply);			
		}

		else{
			int i=0;
			int flag=0;
			oldentries=FailureMap.get(message.Values[0]);

			while(i<message.Reply.size()){
				int j=0;
				Log.e(message.Reply.get(i).Key,message.Reply.get(i).Value);
				while(j<oldentries.size()){
					if(oldentries.get(j).Key.equals(message.Reply.get(i).Key)){
						flag=1;
						if(oldentries.get(j).Version.compareTo(message.Reply.get(i).Version)<0){
							oldentries.set(j,message.Reply.get(i));
						}
						Log.e(message.Reply.get(i).Key,"flag="+flag+"V"+message.Reply.get(i).Version+" "+oldentries.get(j).Version);
						break;
					}
					j++;
				}		

				if(flag!=1){
					oldentries.add(message.Reply.get(i));
					Log.e(message.Reply.get(i).Key,"flag="+flag+"V"+message.Reply.get(i).Version+" ");
				}
				i++;
				flag=0;
			}
			FailureMap.put(message.Values[0], oldentries);
		}

		Log.e("gave up the lock",message.Values[0]);
	}


	private class ServerTask implements Runnable {

		ServerSocket serverSocket;
		protected void StartServer(ServerSocket socket) {
			this.serverSocket=socket;
			Thread thread1 = new Thread(this);
			thread1.start(); 	
		}

		@Override
		public void run() {

			while(true){
				try {	
					Log.e("my port","server is listening");
					Log.e("my port",myPort);

					Socket client=serverSocket.accept();

					ObjectInputStream instream = new ObjectInputStream(client.getInputStream());

					Message message;
					message = (Message)instream.readObject();
					instream.close();
					client.close();

					if(message.MessageType.equals("Insert")){
						Peer_insert(buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider"), message);
					}

					else if(message.MessageType.equals("ReplicateInsert")){
						replicateInsert(message);
					}

					else if(message.MessageType.equals("Query")){
						Peer_Query(message);
					}

					else if(message.MessageType.equals("WriteQ")){
						HandleQuorumReply(message.Values);
					}

					else if(message.MessageType.equals("QueryReply")){
						HandleQueryReply(message);
					}

					else if(message.MessageType.equals("FailureReply")){
						HandleFailureReply(message);
					}

					else if(message.MessageType.equals("Delete")){
						Peer_Delete(message);
					}
				}
				catch (IOException e) {
					// TODO Auto-generated catch block
					if(e.getMessage()!=null){
					}
				} 
				catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}	
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
}
