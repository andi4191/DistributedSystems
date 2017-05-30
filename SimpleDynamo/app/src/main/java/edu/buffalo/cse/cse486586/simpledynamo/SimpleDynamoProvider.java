package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.util.LongSparseArray;

import static android.content.Context.INPUT_METHOD_SERVICE;
import static android.content.Context.TELEPHONY_SERVICE;


class ValueStorage implements Comparable<ValueStorage> {

	public String value;
	public long version;


	@Override
	public int compareTo(ValueStorage another) {
		return (int) (this.version - another.version);
	}
}

public class SimpleDynamoProvider extends ContentProvider {

	static Lock lock = new ReentrantLock();
	int TIME_OUT = 1000;
	//static Map<String , String> quer = new ConcurrentHashMap<String , String>();
	static Map<String, ValueStorage> quer = new ConcurrentHashMap<String, ValueStorage>();
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private SimpleDynamoDB db = null;
	String TAGC = "CLIENT";
	String TAGS = "SERVER";
	static final int SERVER_PORT = 10000;
	static Uri myUri=null;
	String myPort = null;
	String myID = null;
	String REPLICATE = "0";
	String NON_REPLICATE = "1";

	Integer mySucc1 = 0;
	Integer mySucc2 = 0;
	int MSGTYPE = 0;
	String ALL = "*";
	String CURRENT = "@";

	private HashMap<String, String> portMap = new HashMap<String, String>();
	private HashMap<Integer, Integer> map_ports = new HashMap<Integer, Integer>();
	private HashMap<String, Integer> nodeHash = new HashMap<String, Integer>();


	int REMOTE_PORTS[] = {11108, 11112, 11116, 11120, 11124};
	List<String> nodes = new ArrayList<String>();
	static HashMap<String, String> queries = new HashMap<String, String>();

	boolean queryRespReceived = false;
	String queryResp = null;

	public enum MsgType {
		SIGN_IN_REQ(1), SIGN_IN_RESP(2), UPDATE_NODE(3), INSERT(4), QUERY_REQ(5), QUERY_RESP(6), UPDATE_TABLE(7), DELETE(8), REPLICATE(9);

		private int value;

		MsgType(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}
	}



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		Log.i(TAG,"DELETE func selection: "+selection);
		if(selection.equals(CURRENT)) {
			handleDelete(selection);
		}
		else if(selection.equals(ALL)) {

			Log.i(TAGC, "SELECT_ALL query");
			handleDelete(selection);
			for(int i=0;i<REMOTE_PORTS.length;i++) {

				if (REMOTE_PORTS[i] == Integer.parseInt(myPort)) continue;

				sendDeleteMsg(selection, Integer.toString(REMOTE_PORTS[i]));
			}

		}
		else {

			db.deleteFromDB(selection);
/*
			//Its key specific query

			//Determine the potential key store keepers for this key. If current key is also a part then skip req and resp

			Log.i(TAGC,"KEY SPECIFIC DELETE");

			String resp = getKeyPositions(selection);
			String []tok = resp.split(",");
			String REPLICAS[] = {Integer.toString(nodeHash.get(tok[0])), Integer.toString(nodeHash.get(tok[1])), Integer.toString(nodeHash.get(tok[2]))};

			Log.i(TAG,"REPLICAS length "+ Integer.toString(REPLICAS.length-1));
			//for(int i=REPLICAS.length-1;i>=0;i--) {
			for(int i=0;i<REPLICAS.length;i++) {

				if(REPLICAS[i].equals(myPort)) {
					Log.i(TAG,"Deleting from own db "+myPort);
					db.deleteFromDB(selection);
				}
				else {
					sendDeleteMsg(selection, REPLICAS[i]);
				}

			}
*/			Log.i(TAG, "Deleted key from all the replicas "+selection);
		}

		return 0;
	}

	public boolean isAcceptable(String key, String port) {

		List<String> lis = new ArrayList<String>();
		for(String e: nodeHash.keySet()) {
			lis.add(e);
		}
		lis.add(key);
		Collections.sort(lis);
		int index = 0;
		for(int i=0;i<lis.size();i++) {
			if(lis.get(i).equals(key)) {
				index = i;
				break;
			}
		}
		boolean flag = false;
		for(int i=1;i<4;i++) {
			if(lis.get((index+i)%lis.size()).equals(port)) {
				flag = true;
				break;
			}
		}
		Log.i(TAG,"flag: " + flag+ "index "+ index + "lis "+lis.toString());
		return flag;
	}


	public void sendDeleteMsg(String selection, String remotePort) {

		Socket socket = null;
		String req = Integer.toString(MsgType.DELETE.getValue())+","+selection;
		DataInputStream is = null;
		DataOutputStream os = null;
		try {

			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(remotePort));

			//Log.i(TAGC, "socket " + socket);
			socket.setSoTimeout(TIME_OUT);

			is = new DataInputStream(socket.getInputStream());
			os = new DataOutputStream(socket.getOutputStream());

			String reply = "NONE";


			Log.i(TAGC, "Before sending the deletemsg " + req + " to " + remotePort);
			os.writeUTF(req);
			Log.i(TAGC, "Sent the msg " + req + " to " + remotePort);
			do {
				Log.i(TAGC, "Awaiting response ");
				reply = is.readUTF();
				Log.i(TAGC, "Reply rcvd as " + reply);
			} while (reply.equals("NONE"));


			socket.close();
			if(is!=null) is.close();
			if(os!=null) os.close();
		} catch (Exception e) {
			//socket.close();

		}
	}

	public void handleDelete(String selection) {

			db.deleteFromDB(selection);

	}


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key = (String) values.get("key");
		String value = (String) values.get("value");
		String res = null;
		String hashedKey  = null;
		try {
			hashedKey = genHash(key);
			 res = getKeyPositions(hashedKey);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		Log.i(TAG, "getKeyPosition for "+key + " hashed as "+ hashedKey);

		String ports[] = res.split(",");
		Log.i(TAG," checkpoint1 "+res + " ports[0] "+ports[0]);
		Log.i(TAG, "nodeHash "+nodeHash.toString());
		Integer dest = nodeHash.get(ports[0]);
		Log.i(TAG,"dest "+dest);
		long time = System.currentTimeMillis();
		String req = null;

		String []STORAGE = {Integer.toString(nodeHash.get(ports[0])), Integer.toString(nodeHash.get(ports[1])), Integer.toString(nodeHash.get(ports[2]))};

		//req = Integer.toString(MsgType.INSERT.getValue())+","+key + "-" + value + "-" + time + "," + ports[0] + "," + ports[1] + "," + ports[2];
		req = Integer.toString(MsgType.INSERT.getValue())+","+key + "-" + value + "-" + time;

		for(int i=0;i<STORAGE.length;i++) {

			if(STORAGE[i].equals(myPort)) {
				//db.insertToDB(key, value, time);
				insertDB(key, value, time);
				Log.i(TAG, "inserted into own DB " + key);
			}
			else {

				try {
					sendInsertMsg(req, STORAGE[i]);
				} catch (IOException e) {
					e.printStackTrace();
				}
				Log.i(TAG, "Insert being sent to " + STORAGE[i]);
				//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.INSERT.getValue()), req, STORAGE[i]);
			}

		}
		//Log.i(TAG, "Forwarding INSERT req to coordinator "+ Integer.toString(nodeHash.get(ports[0])) + " or if it fails then " + Integer.toString(nodeHash.get(ports[1])));
		//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.INSERT.getValue()), req, Integer.toString(nodeHash.get(ports[0])), Integer.toString(nodeHash.get(ports[1])));


		/*
		if (Integer.toString(dest).equals(myPort)) {

			Log.i(TAG, "Own port is the destination so insert into DB "+myPort);

            //Insert in own's DB and forward to 2 successors in background.

            db.insertToDB(key, value, time);
            //req = key + "-" + value + "-" + time + "," + ports[0] + "," + ports[1] + "," + ports[2] + "," + REPLICATE;
            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.INSERT.getValue()), req, ports[1], ports[2]);

            req = key + "-" + value + "-" + time + "," + ports[1] + "," + ports[2];
			Log.i(TAG,"Sending REPLICATE msg to "+Integer.toString(nodeHash.get(ports[1])) + "," + Integer.toString(nodeHash.get(ports[2])));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.REPLICATE.getValue()), req, Integer.toString(nodeHash.get(ports[1])), Integer.toString(nodeHash.get(ports[2])));
        } else {
            //Need to send it to coordinator for further processing
            req = key + "-" + value + "-" + time + "," + ports[0] + "," + ports[1] + "," + ports[2];
			Log.i(TAG, "Forwarding INSERT req to coordinator "+ Integer.toString(nodeHash.get(ports[0])) + " or if it fails then " + Integer.toString(nodeHash.get(ports[1])));
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.INSERT.getValue()), req, Integer.toString(nodeHash.get(ports[0])), Integer.toString(nodeHash.get(ports[1])));

        }
		*/

		return null;
	}

	public void sendInsertMsg(String ... msg) throws IOException {

		Socket socket = null;
		String remotePort = msg[1];
		String req = msg[0];
		DataInputStream is = null;
		DataOutputStream os = null;
		try {

			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					Integer.parseInt(remotePort));

			//Log.i(TAGC, "socket " + socket);
			socket.setSoTimeout(TIME_OUT);

			is = new DataInputStream(socket.getInputStream());
			os = new DataOutputStream(socket.getOutputStream());

			String reply = "NONE";


			Log.i(TAGC, "Before sending the insertmsg " + req + " to " + remotePort);
			os.writeUTF(req);
			Log.i(TAGC, "Sent the msg " + req + " to " + remotePort);
			do {
				Log.i(TAGC, "Awaiting response ");
				reply = is.readUTF();
				Log.i(TAGC, "Reply rcvd as " + reply);
			} while (reply.equals("NONE"));


			socket.close();
			if(is!=null) is.close();
			if(os!=null) os.close();
		} catch (Exception e) {

			if(is!=null) is.close();
			if(os!=null) os.close();
			socket.close();

		}
	}

		@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		myUri = buildUri("content","content://edu.buffalo.cse.cse486586.simpledynamo.provider");

		Log.i(TAG, "onCreate start");

		try {

			ServerSocket serverSock = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSock);
			Log.i(TAG,"Server Thread Spwaned");
		} catch (Exception e) {

			Log.e(TAG, "Error creating Server Socket");
			return false;
		}
		Log.i(TAG,"after creation of server");

		try {
			nodeHash.put(genHash("5554"), 11108);
			nodeHash.put(genHash("5556"), 11112);
			nodeHash.put(genHash("5558"), 11116);
			nodeHash.put(genHash("5560"), 11120);
			nodeHash.put(genHash("5562"), 11124);


		} catch(NoSuchAlgorithmException e) {

		}

		map_ports.put(11108, 5554);
		map_ports.put(11112, 5556);
		map_ports.put(11116, 5558);
		map_ports.put(11120, 5560);
		map_ports.put(11124, 5562);

		//Send notification to successors only.
		try {
			myID = genHash(Integer.toString(map_ports.get(Integer.parseInt(myPort))));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		db = new SimpleDynamoDB(getContext());
		if(db.doesExist()) {
			Log.i(TAG,"Records already exist in db so initiating SIGN_IN sequence");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.SIGN_IN_REQ.getValue()), myPort);
		}

		String ret = getNodePositions(myID);
		String []tok = ret.split(",");
		mySucc1 = nodeHash.get(tok[0]);
		mySucc2 = nodeHash.get(tok[1]);

		Log.i(TAG, "onCreate completed");


		return false;
	}

	public String getPredecessor(String key) {

		List<String> hash = new ArrayList<String>();
		for(String s: nodeHash.keySet()) {
			hash.add(s);
		}

		try {
			hash.add(genHash(key));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Collections.sort(hash);
		int index = -1;
		for(int i=0;i<hash.size();i++) {
			if(hash.get(i).equals(key)) {
				index = i;
			}
		}

		String ret = hash.get((index-1)%nodeHash.size());
		// + "," + hash.get((index+2)%nodeHash.size()) + "," + hash.get((index+3)%nodeHash.size());
		return ret;
	}

	public String checkKeysInRange(String key) {
		String SELECT_ALL = "*";
		Cursor curs = retrieveFromDB(SELECT_ALL);

		String pkey = getPredecessor(key);
		String ret = null;
		curs.moveToFirst();
		if(curs!=null) {
			do {
				    String k = curs.getString(curs.getColumnIndex("key"));
					if(k.compareTo(key)<=0 && k.compareTo(pkey)>0) {
						ret = ret + k + "-" + curs.getString(curs.getColumnIndex("VALUE")) + "-" + curs.getString(curs.getColumnIndex("VERSION")) + ":";
					}
			} while (curs.moveToNext());
		}

		ret = ret.substring(0, ret.length()-1);

		return ret;
	}

	public Cursor retrieveFromDB(String selectionArg) {

		lock.lock();
		Cursor curs = null;
		try {
			curs = db.retrieveFromDB(selectionArg);
			Log.i(TAG, "retrinProvider " + curs.getCount());
		} finally {
			lock.unlock();
		}
		return curs;
	}

	public String getNodePositions(String key) {

		//Will work for NodeIds in the same way
		List<String> hash = new ArrayList<String>();
		for(String s: nodeHash.keySet()) {
			hash.add(s);
		}

		Collections.sort(hash);
		int index = -1;
		for(int i=0;i<hash.size();i++) {
			if(hash.get(i).equals(key)) {
				index = i;
				break;
			}
		}

		String ret = hash.get((index+1)%nodeHash.size()) + "," + hash.get((index+2)%nodeHash.size()) + "," + hash.get((index+3)%nodeHash.size());
		return ret;
	}


	public String getKeyPositions(String key) {

		//Will work for NodeIds in the same way
		List<String> hash = new ArrayList<String>();
		for(String s: nodeHash.keySet()) {
			hash.add(s);
			Log.i(TAG, "Adding to hash list "+s);
		}

		hash.add(key);

		Collections.sort(hash);
		int index = -1;
		Log.i(TAG,"Check sortedHash with key "+hash.toString());
		for(int i=0;i<hash.size();i++) {
			if(hash.get(i).equals(key)) {
				index = i;
				break;
			}
		}

		String ret = hash.get((index+1)%hash.size()) + "," + hash.get((index+2)%hash.size()) + "," + hash.get((index+3)%hash.size());
		Log.i(TAG, "Returning from getKeyPosition: "+ret + "for key "+key);
		return ret;
	}
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub


		Log.i(TAG,"QUERY func selection: "+selection);
		MatrixCursor curs = null;
		String updatedKeys = null;
		/*
		* 1. Check querytype
		* 	i) if key-specific, determine the coordinator for that key.
		*   a. if present in own's DB, then proceed for handleQuery with selection as key
 		* 	b. if not own's then forward Query Req to the coordinator and wait for reply back.
 		*
 		* 	ii) if @ or *, then retrieve own's db records.
 		* 	a. poll for successors, determine the updated version and add to cursor.
 			b. if *, then forward the updated ones as well along with QUERY_REQ, this will save some time in processing
 			c. if @ then simply return the updated records.
		* */

		//Need to poll for each key so better to take * from both successors and decide which one should be picked up.

		//int count = db.retrieveFromDB(ALL).getCount();
		if(selection.equals(CURRENT) ) {
			curs = (MatrixCursor) handleQuery(selection);
		}
		else if(selection.equals(ALL)) {

			Log.i(TAGC, "SELECT_ALL query");
			curs = (MatrixCursor) handleQuery(selection);
			for(int i=0;i<REMOTE_PORTS.length;i++) {

				if(REMOTE_PORTS[i] == Integer.parseInt(myPort)) continue;

				String reply = queryMsgAll(selection, Integer.toString(REMOTE_PORTS[i]));

				Log.i(TAG,"reply received for selec_all query is "+reply);
				if(reply == null || reply.equals("NONE")) continue;

				String[] toke = reply.split(",");
				boolean flag = false;
				for (int j = 0; j < toke.length; j++) {
					String[] token = toke[j].split("-");
					long polledVersion = Long.parseLong(token[2]);
					String key = token[0];
					String valu = token[1];
					curs.addRow(new Object[] {key, valu});
					try {
						if(isAcceptable(genHash(key), genHash(myPort))) {
                            insertDB(key, valu, polledVersion);
                        }
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
					//updateIfExists(key, valu, polledVersion);
				}

			}

		}
		else {


			//Its key specific query

			//Determine the potential key store keepers for this key. If current key is also a part then skip req and resp

			Log.i(TAGC,"KEY SPECIFIC QUERY");

			String resp = null;
			try {
				resp = getKeyPositions(genHash(selection));
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			String []tok = resp.split(",");
			String REPLICAS[] = {Integer.toString(nodeHash.get(tok[0])), Integer.toString(nodeHash.get(tok[1])), Integer.toString(nodeHash.get(tok[2]))};

			String value = null;
			long version = 0;

			for(int i=0;i<REPLICAS.length;i++) {

				if(REPLICAS[i].equals(myPort)) {
					Cursor c = retrieveFromDB(selection);
					if(c!=null && c.getCount()>0) {
						c.moveToFirst();
						value = c.getString(c.getColumnIndex("VALUE"));
						version = c.getLong(c.getColumnIndex("VERSION"));
					}
				}
				else {
					String reply = queryMsgAll(selection, REPLICAS[i]);
					Log.i(TAG, "Resp RCVED " + reply);
					if (reply == null || reply.equals("NONE") || reply.equals("")) continue;
					String[] token = reply.split("-");

					long ver = Long.parseLong(token[2]);
					Log.i(TAG, "Checking the values " + version + ", ver " + ver);
					if (version <= ver) {
						value = token[1];
						version = ver;
					}
				}

			}
			curs = new MatrixCursor(new String[]{"key", "value"});
			curs.moveToFirst();
			curs.addRow(new Object[] {selection, value});
		}


		Log.i(TAG, "Query returning curs rows: "+curs.getCount());
		return curs;

	}

	public Cursor handleQuery(String queryType) {

		HashMap<String, ValueStorage> currDB = new HashMap<String, ValueStorage>();
		Cursor curs = retrieveFromDB("*");

		if(curs!=null && curs.getCount()>0) {
			curs.moveToFirst();
			do {

				ValueStorage ob = new ValueStorage();
				//Log.i(TAG,"column count "+curs.getColumnCount() + " with names "+curs.getColumnName(0)+"," + curs.getColumnName(1) +"," +curs.getColumnName(2) + " retCount: "+ curs.getCount());
				String k = curs.getString(curs.getColumnIndex("key"));
				ob.value = curs.getString(curs.getColumnIndex("VALUE"));
				ob.version = curs.getLong(curs.getColumnIndex("VERSION"));
				currDB.put(k, ob);

			} while (curs.moveToNext());
		}
		Log.i(TAG,"checkkk crrDB " + currDB.keySet().toString());



		Log.i(TAG,"mS1: "+mySucc1+" mS2: "+mySucc2);
		String []REPLICAS = {Integer.toString(mySucc1), Integer.toString(mySucc2)};

		if(queryType.equals(CURRENT)) queryType = ALL;
		for(int k = 0;k < REPLICAS.length; k++) {
			String resp = pollMsg(queryType, REPLICAS[k], 1);
			Log.i(TAG,"Resp rcvd as "+resp);
			HashMap<String, ValueStorage> secondDB = new HashMap<String, ValueStorage>();
			if(resp.equals("NONE") || resp.equals("")) continue;
			String[] token = resp.split(",");
			for (int i = 0; i < token.length; i++) {
				String[] tok = token[i].split("-");
				ValueStorage obj = new ValueStorage();
				String ky = tok[0];
				obj.value = tok[1];
				obj.version = Long.parseLong(tok[2]);
				secondDB.put(ky, obj);

			}

			Log.i(TAG, "Check current DB" + currDB.keySet().toString());
			Log.i(TAG, "Check second DB" + secondDB.keySet().toString());
			HashMap<String, ValueStorage> tmp = new HashMap<String, ValueStorage>();
			for(String e: secondDB.keySet()) {
				long v1 = 0;
				long v2 = 0;
				try {
					if(currDB.containsKey(e)) {
						v1 = currDB.get(e).version;
					}
					v2 = secondDB.get(e).version;
					if(isAcceptable(genHash(e), myID) && v1<=v2) {
						tmp.put(e, secondDB.get(e));
						insertDB(e, secondDB.get(e).value, v2);
                    }
//					if(v1 <= v2) {
//
//					}
				} catch (NoSuchAlgorithmException e1) {
					e1.printStackTrace();
				}
			}
			/*
			for (String e : currDB.keySet()) {
				long v1 = currDB.get(e).version;
				if(secondDB.containsKey(e)) {
					long v2 = secondDB.get(e).version;
					if (v2 != 0 && v1 < v2) {
						//currDB.remove(e);
						//currDB.put(e, secondDB.get(e));
						tmp.put(e, secondDB.get(e));
						//db.insertToDB(e, tmp.get(e).value, v2);
						insertDB(e, tmp.get(e).value, v2);
					}
				}
			}
			*/

			for(String e: tmp.keySet()) {
				currDB.remove(e);
				currDB.put(e, tmp.get(e));
			}
		}


		MatrixCursor cur = new MatrixCursor(new String[]{"key", "value"});
		for(String s: currDB.keySet()) {
			cur.moveToFirst();
			cur.addRow(new Object[]{s, currDB.get(s).value});
			insertDB(s, currDB.get(s).value, currDB.get(s).version);
		}

		Log.i(TAG,"Returning number of rows: "+cur.getCount());
		return cur;

	}


	//For querying the msg for *
	private String queryMsgAll(String selection, String remotePort) {

		Socket socket = null;
		String reply = "NONE";
		String req = null;

		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
			socket.setSoTimeout(TIME_OUT);

			DataInputStream is = new DataInputStream(socket.getInputStream());
			DataOutputStream os = new DataOutputStream(socket.getOutputStream());
			req = MsgType.QUERY_REQ.getValue() + "," + myPort + "," + selection;

			Log.i(TAG, "Sending req: " + req + " remotePort " + remotePort);
			os.writeUTF(req);
			do {
				Log.i(TAG, "Awaiting ACK fro quer_req");
				reply = is.readUTF();
				Log.i(TAG, "resp rcvd "+reply);
			} while (reply.equals("NONE"));


			if (is != null) is.close();
			if (os != null) os.close();
			socket.close();

		} catch (Exception e) {
			e.printStackTrace();
			Log.i(TAG,"Caught exception "+e);
			reply = null;
		}
		return reply;
	}


	private String pollMsg(String selection, String port, int ispoll) {

		Socket socket = null;
		String reply = "NONE";
		String queriedKey = selection;
		String req = null;
		boolean isNodeDead = false;
		try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(port));

				socket.setSoTimeout(TIME_OUT);

				DataInputStream is = new DataInputStream(socket.getInputStream());
				DataOutputStream os = new DataOutputStream(socket.getOutputStream());


				req = MsgType.QUERY_REQ.getValue() + "," + myPort + "," + selection + ","+ispoll;

				Log.i(TAG, "Sending req: " + req + " remotePort " + port);

				reply = "NONE";
				os.writeUTF(req);
				do {
					Log.i(TAG, "Awaiting ACK fro quer_req");
					reply = is.readUTF();
				} while (reply.equals("NONE"));




				if (is != null) is.close();
				if (os != null) os.close();
				socket.close();
			} catch (SocketTimeoutException e) {
				isNodeDead = true;
			}
			catch (Exception e) {
				e.printStackTrace();
				Log.e(TAG, "Exception caught at 11");
			}

		return reply;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

			Socket servSocket = null;
			do {
				try {
					Log.i(TAGS,"checkpoint2");
					servSocket = serverSocket.accept();
					Log.i(TAGS,"checkpoint3");
					DataInputStream is = new DataInputStream(servSocket.getInputStream());
					DataOutputStream os = new DataOutputStream(servSocket.getOutputStream());

					String str = null;
					String reply = null;
					String flag = null;

					Log.i(TAGS, "Within server before while");

					while (null != (str = is.readUTF())) {

						//check type of message
						Log.i(TAGS, "MSG RCVD " + str);
						String[] tokens = str.split(",");

						int msgType = Integer.parseInt(tokens[MSGTYPE]);


						if (msgType == MsgType.SIGN_IN_REQ.getValue()) {

                        /*  Message structure SIGN_IN_REQ, RportNo, hashedVal
                        *
                        * */


							// SIGN_IN_RESP
							//Check if there are any keys which belonged to predecessor are present in current node
							//If yes reply back with key - value pairs else reply back with ack

							String fromPort = tokens[1];
							String hashedVal = tokens[2];

							List<String> lis = new ArrayList<String>();
							for(String e : nodeHash.keySet()) {
								lis.add(e);
							}
							lis.add(hashedVal);

							Collections.sort(lis);

							//Now determine if current node is within the proximit of 2 predecessors or 2 successors

							boolean ispred = false;
							int index = 0;
							for(int i=0;i<lis.size();i++) {
								if(lis.get(i).equals(hashedVal)) {
									index = i;
									break;
								}
							}

							int prev_index1 = index -1;
							int prev_index2 = index -2;
							if(prev_index1 < 0) {
								prev_index1 = lis.size() + prev_index1;
							}


							if(prev_index2 < 0) {
								prev_index2 = lis.size() + prev_index2;
							}

							if(myID.equals(lis.get((prev_index1)% lis.size())) || myID.equals(lis.get((prev_index2)%lis.size()))) {
									ispred = true;
							}

							//if current node is among the predecessor of the recovery node then select all the keys which should be present in own node


							String myPred = getMyPredecessor(myID);

							String resp = "";
							Cursor cur = retrieveFromDB(ALL);
							if(cur!=null && cur.getCount()>0) {
								cur.moveToFirst();
								do {
									String k = cur.getString(cur.getColumnIndex("key"));
									String v = cur.getString(cur.getColumnIndex("VALUE"));
									long version = cur.getLong(cur.getColumnIndex("VERSION"));

									boolean flg = isAcceptable(genHash(k), hashedVal);
									Log.i(TAGS,"is Acceptable "+ flg + " k-v-ver: "+k+"-"+v+"-"+version);
									if(flg) {
										resp = resp + k + "-" + v + "-" + version + ",";
									}

									/*
									Log.i(TAGS," isPred "+ispred + " k-val-ver "+k+"-"+v+"-"+version);
									if(ispred == true) {
										if((myID.compareTo(myPred) > 0 && k.compareTo(myID) <=0 && k.compareTo(myPred) > 0) || (myID.compareTo(myPred) < 0 && (k.compareTo(myPred) > 0 || k.compareTo(myID)<=0))) {
										//if (k.compareTo(myID) <=0 && k.compareTo(myPred) > 0) {
											resp = k + "-" + v + "-" + version +",";
											Log.i(TAGS,"Pred case Appending "+resp);
										}
									}
									else {

										String hPred = getMyPredecessor(hashedVal);
										if((hPred.compareTo(hashedVal) < 0 && k.compareTo(hashedVal)<=0 && k.compareTo(hPred)>0) || (hPred.compareTo(hashedVal) > 0 && (k.compareTo(hPred)>0 || (k.compareTo(hashedVal) <=0)))) {
											resp = k + "-" + v + "-" + version + ",";
											Log.i(TAGS,"Succ case Appending "+resp);
										}

									}
									*/

								}while(cur.moveToNext());

								if(resp.length()>0) {
									resp = resp.substring(0, resp.length() - 1);
								}
							}

							if(resp.length()==0) {
								resp = "NONE";
							}
							os.writeUTF(resp);
							Log.i(TAGS,"Replied back with SIGN_IN_RESP "+resp);

						}
						else if (msgType == MsgType.INSERT.getValue()) {



							String st = tokens[1];
							String[] toks = st.split("-");
							String key = toks[0];
							String value = toks[1];
							long version = Long.parseLong(toks[2]);
							//db.insertToDB(key, value, version);
							insertDB(key, value, version);
							os.writeUTF("ACK");
							/*

							os.writeUTF("ACK");
							String st = tokens[1];
							String[] toks = st.split("-");
							String key = toks[0];
							String value = toks[1];
							long version = Long.parseLong(toks[2]);

							String RPORT[] = {tokens[2], tokens[3], tokens[4]};

							//Check if current node is the coordinator or proxy for coordinator
							boolean coord = false, midEle = false;
							Log.i(TAGS,"myID: "+myID + " 2,3,4: "+tokens[2]+","+tokens[3]+","+tokens[4]);
							if(myID.equals(tokens[2])) {
								coord = true;
							}
							else if(myID.equals(tokens[3])) {
								midEle = true;
							}
							Log.i(TAGS," check flags cD "+coord + ", mE " + midEle);
							//version = version + 1;
							db.insertToDB(key, value, version);

							Log.i(TAGS,"inserting into the db from server");
							String req = key + "-"+ value + "-" + version;
							if(coord == true) {

								req = req + "," + nodeHash.get(tokens[3]) + "," + nodeHash.get(tokens[4]);
								publishProgress(Integer.toString(MsgType.REPLICATE.getValue()), req, myPort);
							}
							else if(midEle == true) {
								Log.i(TAGS,"Means middle element "+midEle);
								req = req + "," + nodeHash.get(tokens[4]) + "," + nodeHash.get(tokens[4]);
								publishProgress(Integer.toString(MsgType.REPLICATE.getValue()), req, myPort);
							}
							else {
								Log.i(TAGS,"Its the last replica");
								}
							*/

						}


						else if(msgType == MsgType.QUERY_RESP.getValue()) {
							os.writeUTF("ACK");
							String requester = tokens[1];
							String selection = tokens[2];
							String resp = tokens[3];

							Log.i(TAGS,"QUERY_RESP received requester: "+requester+" myPort: "+myPort+" resp: "+resp);
							if(myPort.equals(requester)) {


								Log.i(TAGS,"Resp accepted in queue");
							}

						}

						else if (msgType == MsgType.QUERY_REQ.getValue()) {
							//os.writeUTF("ACK");

							String requester = tokens[1];
							String selection = tokens[2];
							//String ispoll = tokens[3];

							Log.i(TAGS,"QUERY_REQ message received requester: "+requester+" selection: "+selection);

							MatrixCursor curs = null;
							String resp = "";
							if(selection.equals(ALL)) {

								Log.i(TAGS,"select_all checkpoint");
								Cursor c = retrieveFromDB(selection);

								//curs = (MatrixCursor) retrieveFromDB(selection);
								Log.i(TAGS,"query_req select_all" + c.getCount());
								if(c != null && c.getCount()>0) {
									c.moveToFirst();
									do {

										String k = c.getString(c.getColumnIndex("key"));
										String v = c.getString(c.getColumnIndex("VALUE"));
										resp = resp + k + "-" + v + "-" + c.getLong(c.getColumnIndex("VERSION")) + ",";
									}while(c.moveToNext());
									resp = resp.substring(0, resp.length()-1);
								}


							}
							else {   //Key specific query
								//Pick response from the own hashTable and replyback

								Cursor c = retrieveFromDB(selection);
								Log.i(TAGS,"Key specific query_req received at "+myPort + " retr "+c.getCount());

								if(c.getCount()>0 && c!=null) {
									//curs = (MatrixCursor) retrieveFromDB(selection);
									c.moveToFirst();
									String k = c.getString(c.getColumnIndex("key"));
									String v = c.getString(c.getColumnIndex("VALUE"));
									long version = c.getLong(c.getColumnIndex("VERSION"));
									resp = k + "-" + v + "-" + version;
								}
							}



							Log.i(TAGS,"check if returned");
								//publishProgress(Integer.toString(MsgType.QUERY_RESP.getValue()), resp, requester, selection, myPort);
							os.writeUTF(resp);
							Log.i(TAGS,"Response to Query req for key "+selection+" sent as "+resp);


						}
						else if(msgType == MsgType.DELETE.getValue()) {

							String selection = tokens[1];
							db.deleteFromDB(selection);
							os.writeUTF("ACK");
							/*
							os.writeUTF("ACK");
							String st = tokens[1];
							boolean fwd = false;
							if(st.equals(ALL)) {
								db.clear();
								fwd = true;
							}
							else {
								//Key Specific
								if(db.containsKey(st)) {
									db.remove(st);
								}
								else
									fwd = true;

							}
							if(fwd) {
								publishProgress(Integer.toString(MsgType.DELETE.getValue()), st, mySuccessorPort, myPort);
							}
*/
						}
						else if(msgType == MsgType.REPLICATE.getValue()) {

							os.writeUTF("ACK");
							String st = tokens[1];
							Log.i(TAGS, "REPLICATE msg received as "+st);
							String []toks = st.split(",");
							String []tok = toks[0].split("-");
							String k = tok[0];
							String v = tok[1];
							long version = Long.parseLong(tok[2]);
							//db.insertToDB(k, v, version);
							insertDB(k,v,version);

						}



					}

					if(os!=null)  os.close();
					if(is!=null) is.close();


				} catch(EOFException e) {

				}
				catch (Exception e) {
					e.printStackTrace();
					try {
						servSocket.close();
					} catch (IOException e1) {
					}


				}

			}while (servSocket.isConnected()) ;

			return null;

		}

		protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */

			//Check if the task is intended to notify the new predecessor and successor by msgtype as the first parameter
			if(strings[0].equals(Integer.toString(MsgType.SIGN_IN_REQ.getValue()))) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.SIGN_IN_REQ.getValue()),myPort);
			}
//
//			else if(strings[0].equals(Integer.toString(MsgType.UPDATE_NODE.getValue()))) {
//
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.UPDATE_NODE.getValue()), strings[1], strings[2], myPort);
//				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.UPDATE_NODE.getValue()), strings[3] ,strings[4], myPort);
//
//			}
//			else if(strings[0].equals(Integer.toString(MsgType.UPDATE_TABLE.getValue()))) {
//				if(!db.isEmpty()) {
//					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.UPDATE_TABLE.getValue()), strings[1], strings[2], myPort);
//				}
//			}
			else if(strings[0].equals(Integer.toString(MsgType.INSERT.getValue()))) {

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.INSERT.getValue()), strings[1], strings[2], myPort);
			}
			else if(strings[0].equals(Integer.toString(MsgType.QUERY_REQ.getValue()))) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.QUERY_REQ.getValue()), strings[1], strings[2], strings[3], myPort);
			}
			else if(strings[0].equals(Integer.toString(MsgType.QUERY_RESP.getValue()))) {
				//Send the query response to requesting node
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.QUERY_RESP.getValue()), strings[1], strings[2], strings[3], myPort);


			}
			else if(strings[0].equals(Integer.toString(MsgType.DELETE.getValue()))) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.DELETE.getValue()), strings[1], strings[2], myPort);
			}
			else if(strings[0].equals(Integer.toString(MsgType.REPLICATE.getValue()))) {

				String ports[] = strings[1].split(",");

				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.REPLICATE.getValue()), ports[0], ports[1], ports[2], myPort);
			}
			else {
				Log.i(TAGS,"Unknown type of request received");

			}

			return;
		}



		public String getMyPredecessor(String key) {

			List<String> lis = new ArrayList<String>();
			for(String e: nodeHash.keySet()) {
				lis.add(e);
			}
			lis.add(key);
			Collections.sort(lis);
			int index = 0;
			for(int i=0;i<lis.size();i++) {
				if(lis.get(i).equals(key)) {
					index = i;
					break;
				}
			}

			if((index-1) <0) {
				index = (lis.size()+index-1)%lis.size();
			}
			String iD = lis.get(index);
			return iD;
		}

		public void insertIntoDB(String key, String value) {

			ContentValues keyValueToInsert = new ContentValues();
			keyValueToInsert.put("key", key);
			keyValueToInsert.put("value", value);

			try {
				Uri newUri = getContext().getContentResolver().insert(
						myUri,    // assume we already created a Uri object with our provider URI
						keyValueToInsert
				);

			} catch (Exception e) {
				//e.printStackTrace();
				Log.e(TAG, "Exception 12");
			}
		}

	}


	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			String req = null;
			Socket socket = null;
			List<Integer> rPort = new ArrayList<Integer>();
			try {

				int msgToSend = Integer.parseInt(msgs[0]);

				if(msgToSend == MsgType.SIGN_IN_REQ.getValue()) {

					for(int i=0;i<REMOTE_PORTS.length;i++) {
						if(REMOTE_PORTS[i] == Integer.parseInt(myPort)) continue;

						rPort.add(REMOTE_PORTS[i]);

					}

					req = Integer.toString(MsgType.SIGN_IN_REQ.getValue()) + "," + myPort+","+ myID;
				}
				else if(msgToSend == MsgType.INSERT.getValue()) {
					req = Integer.toString(MsgType.INSERT.getValue()) + "," + msgs[1];
					rPort.add(Integer.parseInt(msgs[2]));
					//If first one time out then need to send it to next one
					//rPort.add(Integer.parseInt(msgs[3]));

					//Log.i(TAGC, "INSERT msg " + req +" to "+msgs[2]+" else "+msgs[3]);
					Log.i(TAGC,"INSERT msg "+req+ " to "+msgs[2]);
					//remotePort = msgs[2];


				}
				else if(msgToSend == MsgType.QUERY_REQ.getValue()) {

					req = MsgType.QUERY_REQ.getValue()+","+msgs[2] + ","+msgs[3] + ","+msgs[1];
					//remotePort = mySuccessorPort;


				}
//				else if(msgToSend == MsgType.QUERY_RESP.getValue()) {
//					Log.i(TAG, "Sending QUERY_RESP message to "+msgs[2]);
//					req = MsgType.QUERY_RESP.getValue()+","+msgs[2]+","+msgs[3]+","+msgs[1];
//					//req = msgs[1];
//					//remotePort = msgs[2];
//					rPort.add(Integer.parseInt(msgs[2]));
//				}
				else if(msgToSend == MsgType.DELETE.getValue()) {
					req = Integer.toString(MsgType.DELETE.getValue()) + "," + msgs[1];
					rPort.add(Integer.parseInt(msgs[2]));
					Log.i(TAGC,"Sending DELETE as "+req+" msg to "+msgs[2]);
				}
				else if(msgToSend == MsgType.REPLICATE.getValue()) {

					req = Integer.toString(MsgType.REPLICATE.getValue()) + "," + msgs[1];
					rPort.add(Integer.parseInt(msgs[2]));
					if(!msgs[2].equals(msgs[3])) {
						rPort.add(Integer.parseInt(msgs[3]));
					}
					Log.i(TAGC, "REPLICATE msg to "+msgs[2]+" & "+msgs[3]);

				}


				for(int i=0;i<rPort.size();i++) {

					try{
					String remotePort = Integer.toString(rPort.get(i));

					Log.i(TAGC, "Client side remotePort: " + remotePort + " msgType: " + msgToSend);
					Log.i(TAGC, "msgTosend " + msgToSend);
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));

					socket.setSoTimeout(TIME_OUT);
					//Log.i(TAGC, "socket " + socket);
					DataInputStream is = new DataInputStream(socket.getInputStream());
					DataOutputStream os = new DataOutputStream(socket.getOutputStream());

					String reply = "NONE";


					Log.i(TAGC, "Before sending the msg " + msgToSend + " to " + remotePort);
					os.writeUTF(req);
					Log.i(TAGC, "Sent the msg with msgType: " + msgToSend + " to " + remotePort);
					do {
						Log.i(TAGC, "Awaiting response for " + msgToSend);
						reply = is.readUTF();
						Log.i(TAGC, "Reply rcvd as " + reply);
					} while (reply.equals("NONE"));


					if (msgToSend == MsgType.SIGN_IN_REQ.getValue()) {
						Log.i(TAGC, "SIGN_IN_RESP received as " + reply);
						if (!reply.equals("NONE") || !reply.equals("") || reply!=null) {

							handleSignInResp(reply);
						}
						else {
							Log.e(TAGC,"NONE received "+reply);
						}
					} else {
						Log.i(TAGC, "ACK received for " + msgToSend);
					}
/*
						if(msgToSend == MsgType.INSERT.getValue() && !reply.equals("NONE")) {

							Log.i(TAGC,"inser msg successfull so breaking out of the loop");
							//It means it has been successfull so don't need to send it to the next successor
							if (os != null) os.close();
							if (is != null) is.close();
							socket.close();

							break;
						}
*/
					if (os != null) os.close();
					if (is != null) is.close();
					socket.close();
					} catch (EOFException e) {

					}
					catch(SocketTimeoutException e) {
						Log.i(TAGC, "Socket timedout");

					}

				}

			} catch(Exception e) {
				e.printStackTrace();
				try {
					socket.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}

			return null;
		}


	}

	public void insertDB(String key, String value, long version) {


		lock.lock();
		try {
			db.insertToDB(key, value, version);
		} finally {
			lock.unlock();
		}

	}

	public void updateIfExists(String key, String value, long version) {
		lock.lock();
		try {
			Log.i(TAG,"Updating the table for key "+key+" to value "+value);
			db.updateIfExists(key, value, version);
		} finally {
			lock.unlock();
		}
	}

	public void handleSignInResp(String reply) {

		Log.i("i","handleSignInResp "+reply);
		String []toks = reply.split(",");
		for(int i=0;i<toks.length;i++) {
			String []tokens = toks[i].split("-");
			String k = tokens[0];
			String v = tokens[1];
			long version = Long.parseLong(tokens[2]);

			insertDB(k,v,version);
			//db.insertToDB(k, v, version);

		}


	}

}

