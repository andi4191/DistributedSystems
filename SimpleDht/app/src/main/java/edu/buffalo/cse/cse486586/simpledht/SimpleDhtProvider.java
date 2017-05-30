package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.MGF1ParameterSpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.StrictMode;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

import static android.content.Context.INPUT_METHOD_SERVICE;
import static android.content.Context.TELEPHONY_SERVICE;
class CustomComparator implements Comparator<String> {
    @Override
    public int compare(String s1, String s2) {
        return s1.compareTo(s2);
    }
}


public class SimpleDhtProvider extends ContentProvider {


    static Map<String , String> quer = new ConcurrentHashMap<String , String>();
    //private HashMap<String, String> db;
    static ConcurrentHashMap<String, String> db;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    String TAGC = "Client";
    String TAGS = "SERVER";
    static final int SERVER_PORT = 10000;
    static Uri myUri=null;
    static String myID = null;
    static int masterNode = 11108;


    String mySuccessor = null;
    String myPredecessor = null;    //Will store the hashvalues
    String mySuccessorPort = null, myPredecessorPort = null;
    String myPort = null;

    int MSGTYPE = 0;
    int PORT = 1;
    int VALUE=2;
    int NODE_FLAG = 1;
    int NODE_TYPE_VAL = 2;
    String lastNode = null;
    String ALL = "*";
    String CURRENT = "@";

    private HashMap<String, String> portMap = new HashMap<String, String>();
    private HashMap<Integer, Integer> mp = new HashMap<Integer, Integer>();
    static Comparator<String> comp = new CustomComparator();
    private HashMap<String, Integer> nodeHash = new HashMap<String, Integer>();
    List<String> nodes = new ArrayList<String>();
    static HashMap<String, String> queries = new HashMap<String, String>();
//    static LinkedBlockingDeque<String> quer = new LinkedBlockingDeque<String>();


    //String queriedKey = null;
    boolean queryRespReceived = false;
    String queryResp = null;

        public enum MsgType {
        SIGN_IN_REQ(1), SIGN_IN_RESP(2), UPDATE_NODE(3), INSERT(4), QUERY_REQ(5), QUERY_RESP(6), UPDATE_TABLE(7), DELETE(8);

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

        //Check delete type
        if(selection.equals(CURRENT)) {
            db.clear();
        }
        else if(selection.equals(ALL)) {

            db.clear();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.DELETE.getValue()), selection, mySuccessorPort, myPort);

        }
        else {
            //Key Specific deletion
            if(db.containsKey(selection)) {
                db.remove(selection);
            }
            else {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.DELETE.getValue()), selection, mySuccessorPort, myPort);
            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {


        String key = (String) values.get("key");
        String valu = (String) values.get("value");
        boolean flag = false;

        //if(mySuccessorPort == null) return null;

        Log.i(TAG,"INSERT func myport: "+myPort +" mySuccessorPort: "+mySuccessorPort + " myPredecessorPort: "+ myPredecessorPort +" key: "+key + "myPredecessor: "+myPredecessor+", myID: "+myID +" mySuccessor: "+mySuccessor);
        String hashKey = null;
        try {
            hashKey = genHash(key);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        flag = isAcceptable(hashKey);
        Log.i(TAG,"Decision for insertion "+flag+" hashedKey: "+hashKey+" ");



        if(flag == false) {
            //Forward it to successor node
            String st = MsgType.INSERT.getValue()+","+(String) values.get("key")+"-"+(String) values.get("value");
            Log.i(TAG, "Forwarding the message for INSERT msg: "+st+" mySuccPort: "+mySuccessorPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.INSERT.getValue()), st, mySuccessorPort, myPort);
            Log.i(TAG,"done insert func");
        //    return uri;
        }
        else {
            //Accept the value so insert in own's hash table

            Log.i(TAG,"Accpeting the insert operation at requested avd itself");
            if (db.containsKey(values.get("key"))) {
                Log.i("insert", "key already present hence removing " + values.get("key"));
                db.remove(values.get("key"));
            }
            db.put(values.get("key").toString(), values.get("value").toString());
            //Log.v("insert","insertion done");
          //  return uri;
        }
        return uri;

    }

    @Override
    public boolean onCreate() {

        Log.i(TAG, "onCreate start");

        //db = new HashMap<String, String>();
        db = new ConcurrentHashMap<String, String>();
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myUri = buildUri("content","content://edu.buffalo.cse.cse486586.simpledht.provider");


        mySuccessorPort = myPort;
        myPredecessorPort = myPort;
        myPredecessor = myID;
        myPredecessor = myID;
        try {

            ServerSocket serverSock = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSock);
            Log.i(TAG,"Server Thread Spwaned");
        } catch (IOException e) {

            Log.e(TAG, "Error creating Server Socket");
            return false;
        }
        try {
            myID = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Log.i(TAG,"myID "+myID + ", myPort: "+ myPort + ", portStr: "+portStr);

        try {
            nodeHash.put(genHash("5554"), 11108);
            nodeHash.put(genHash("5556"), 11112);
            nodeHash.put(genHash("5558"), 11116);
            nodeHash.put(genHash("5560"), 11120);
            nodeHash.put(genHash("5562"), 11124);


            mySuccessor = genHash(portStr);
            myPredecessor = genHash(portStr);
            mySuccessorPort = myPort;
            myPredecessorPort = myPort;
            lastNode = mySuccessor; // Same as itself if only master node is coming up
            nodes.add(myID);


        } catch(NoSuchAlgorithmException e) {

        }

        mp.put(11108, 5554);
        mp.put(11112, 5556);
        mp.put(11116, 5558);
        mp.put(11120, 5560);
        mp.put(11124, 5562);




        Log.i(TAG,"Updated the succ and pred as: "+mySuccessorPort + ","+ myPredecessorPort);

        //Send notification to Master node that current node join if own node is not Master node
        if(Integer.parseInt(myPort) != masterNode) {

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.SIGN_IN_REQ.getValue()), myPort);
            Log.i(TAG,"Not the master node so SIGN_IN_REQ needs to be send");
        }
        /*
        else {
            Log.i(TAG,"Master node so handle accordingly");
            //Master node is its own successor and predecessor

            try {
                mySuccessor = genHash(portStr);
                myPredecessor = genHash(portStr);
                mySuccessorPort = myPort;
                myPredecessorPort = myPort;
                lastNode = mySuccessor; // Same as itself if only master node is coming up
                nodes.add(myID);

                Log.i(TAG,"Updated the succ and pred as: "+mySuccessorPort + ","+ myPredecessorPort);

            }
            catch(NoSuchAlgorithmException e) {
                Log.e(TAG,"Exception caught at updation of sucessor or predecessor at master node");
            }
            //Keep a tab of which ports are being added and their corresponding hashValues
            try {
                portMap.put((String) genHash(myPort), myPort);
            } catch (NoSuchAlgorithmException e) {
             //   e.printStackTrace();
            }


        }*/
        Log.i(TAG, "onCreate completed");

        return false;
    }

    public boolean isAcceptable(String key) {

        boolean ret = false;
        if(myID.equals(mySuccessor) && myID.equals(myPredecessor)) {
            ret = true;
        }
        else if(myID.compareTo(myPredecessor) > 0 && key.compareTo(myID) <=0 && key.compareTo(myPredecessor) > 0) {
            ret = true;
        }
        else if(myID.compareTo(myPredecessor) < 0 && (key.compareTo(myPredecessor) > 0 || key.compareTo(myID)<=0)) {
            ret = true;
        }
        return ret;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {


        //Log.i(TAG, "QUERY func mySucc: "+mySuccessorPort+" myPred: "+myPredecessorPort+" myID: "+myID);
        Log.i(TAG,"QUERY func selection: "+selection);
        MatrixCursor curs = new MatrixCursor(new String[]{"key", "value"});
        String queryType = null;


        if(selection.equals(CURRENT)) {
            for(String s: db.keySet()) {
                curs.moveToFirst();
                curs.addRow(new Object[]{s, db.get(s)});
                Log.i(TAG, "Retrieving value from CURRENT node k: "+s+" val: "+db.get(s));
            }
       //     return curs;
        }
        else if(selection.equals(ALL)) {
            curs.moveToFirst();
            for(String s: db.keySet()) {
                curs.addRow(new Object[]{s, db.get(s)});
                Log.i(TAG, "Retrieving value in ALL k: "+s+" val: "+db.get(s));
            }
            //Now get rest of the DHT from other nodes.

            Log.i(TAG,"QUERY ALL myPort: "+myPort+" mySucc: "+mySuccessorPort);
            if(myPort != mySuccessorPort) {
                String resp = forwardMsg(selection);
                String[] tok = resp.split(":");
                Log.i(TAG, "All query request resp: " + resp);
                for (String s : tok) {
                    Log.i(TAG,"checkpoint 1 "+s);
                    if(!s.contains("-")) continue;
                    String[] kv = s.split("-");
                    curs.addRow(new Object[]{kv[0], kv[1]});
                    Log.i(TAG, "ALL fwdREsp #### k: " + kv[0] + " v: " + kv[1]);
                }

                quer.remove(selection);


            }
            //return curs;

                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.QUERY_REQ.getValue()), ALL, myPort, myPort);

        }
        else {
            //Its key specific query

            Log.i(TAG, "Key specific query requested");
            String k = selection;

                curs.moveToFirst();
                String v = db.get(k);
            if(v!=null) {

                curs.addRow(new Object[]{k, v});
                Log.d("query", "query key " + selection + " value retrieved " + v);
                return curs;
            }
            else if(mySuccessorPort!=myPort){

                Log.i(TAG,"Not present in current node. Hence forwarding request");
                String resp = forwardMsg(selection);
                Log.i(TAG,"Resp received after forwarding msg "+resp);
                String []tok = resp.split(",");
                for(String s: tok) {
                    String []kv = s.split("-");
                    curs.addRow(new Object[]{kv[0], kv[1]});
                    Log.i(TAG, "fwdREsp #### k: "+kv[0]+" v: "+kv[1]);
                }
                //queries.remove(selection);
                //queryRespReceived = false;
                //queriedKey = null;
                quer.remove(selection);
                return curs;
            }

        }


        Log.i(TAG, "Query returning curs rows: "+curs.getCount());
        return curs;

    }


    private String forwardMsg(String ... msgs) {

        Socket socket = null;
        String reply = "NONE";
        String selection = msgs[0];
        String queriedKey = selection;
        String req = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(mySuccessorPort));


        DataInputStream is = new DataInputStream(socket.getInputStream());
        DataOutputStream os = new DataOutputStream(socket.getOutputStream());



        req = MsgType.QUERY_REQ.getValue()+","+myPort+","+selection;

        Log.i(TAG, "Sending req: "+req+" remotePort "+mySuccessorPort);


            os.writeUTF(req);
            do {
                Log.i(TAG, "Awaiting ACK fro quer_req");
                reply = is.readUTF();
            }while(reply.equals("NONE"));

            //Now wait until the query response received for currentKey
            do {
                Log.i(TAGC,"Sleeping again as queries resp for key: "+queriedKey+" resp: "+quer.get(queriedKey));
                Thread.sleep(150);
            }while(!quer.containsKey(queriedKey));

                    //queries.get(queriedKey) == null);


            if(is!=null) is.close();
            if(os!=null) os.close();
            socket.close();
        } catch(Exception e) {
            Log.e(TAG,"Exception caught at 11");
        }


        String resp = quer.get(queriedKey);

//
//        for(String s: quer) {
//            if(s.contains(queriedKey)) {
//                String []tok = s.split("#");
//                resp = tok[1];
//                Log.i(TAG,"Check resp "+resp);
//                break;
//            }
//        }
//        if(queries.get(queriedKey)!=null) {
//            resp = queries.get(queriedKey);
//        }

        Log.i(TAG,"Queried Resp rcvd as "+resp);
        return resp;
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

       //     if(masterNode!=Integer.parseInt(myPort)) {
       //         publishProgress(Integer.toString(MsgType.SIGN_IN_REQ.getValue()),myPort);
       //     }

            Socket servSocket = null;
            do {
                try {
                    servSocket = serverSocket.accept();
                    DataInputStream is = new DataInputStream(servSocket.getInputStream());
                    DataOutputStream os = new DataOutputStream(servSocket.getOutputStream());

                    String str = null;
                    String reply = null;
                    String flag = null;


                    while (null != (str = is.readUTF())) {

                        //check type of message
                        Log.i(TAGS, "MSG RCVD " + str);
                        String[] tokens = str.split(",");

                        int msgType = Integer.parseInt(tokens[MSGTYPE]);


                        if (msgType == MsgType.SIGN_IN_REQ.getValue()) {

                        /*  Message structure SIGN_IN_REQ, RportNo, hashedVal
                        *
                        * */


                            String portNo = tokens[PORT];
                            String hashedVal = tokens[VALUE];
                            int node_val = Integer.parseInt(portNo);
                            nodes.add(hashedVal);
                            Collections.sort(nodes);
                            Log.i(TAGS, "SIGN_IN_REQ for port "+node_val+" checkpoint 1" + nodes.toString());
                            int pos = nodes.indexOf(hashedVal);
                            lastNode = nodes.get(nodes.size()-1);
                            Log.i(TAGS, "ln: " + lastNode + " registered with position " + pos);

                            int predPos = (pos-1)%nodes.size();
                            int succPos = (pos+1)%nodes.size();

                            if(predPos < 0) predPos = nodes.size()-1;
                            if(succPos < 0) succPos = nodes.size()-1;

                            Log.i(TAGS," positions P <"+predPos+"> S <"+succPos+">");

                            int predPort = nodeHash.get(nodes.get(predPos));
                            int succPort = nodeHash.get(nodes.get(succPos));

                            //Send new node's corresponding successor and predecesor values to the new node.

                            Log.i(TAGS, "Its pred " + predPort + ", succ " + succPort);
                            reply = MsgType.SIGN_IN_RESP.getValue() + "," + predPort + "," + succPort;
                            Log.i(TAGS, "SIGN_IN_RESP as " + reply);
                            os.writeUTF(reply);
                            Log.i(TAGS, "SIGN_IN_RESP sent from server");

                            String toPred = MsgType.UPDATE_NODE.getValue() + "," + "SUCCESSOR," + node_val;
                            String toSucc = MsgType.UPDATE_NODE.getValue() + "," + "PREDECESSOR," + node_val;


                            publishProgress(Integer.toString(MsgType.UPDATE_NODE.getValue()), toPred, Integer.toString(predPort), toSucc, Integer.toString(succPort));

                        } else if (msgType == MsgType.UPDATE_NODE.getValue()) {

                        /*
                        *   Need to update the predecessor and successor depending upon the flag values from the msg
                        *
                        *   If the flag is for Predecessor then simply update the successor with new node's value
                        *
                        *   If the flag is for Successor then update the predecessor with new node value and partition own's hashtable based on new node value and send it to new node as a UPDATE_TABLE message.
                        *
                        * */


                            os.writeUTF("ACK");
                            Log.i(TAGS,"Acknowledge sent <UPDATE_NODE>!!");
                            flag = tokens[NODE_FLAG];
                            int val = Integer.parseInt(tokens[NODE_TYPE_VAL]);
                            Log.i(TAGS, "UPDATE_NODE msg received flag: "+flag + ", val: " + val);

                            if (flag.equals("SUCCESSOR")) { //means the message has come for PREDECESSOR stating the Successor port value
                                //Simply update the successor value as new node's hash value
                                mySuccessorPort = Integer.toString(val);
                                mySuccessor = genHash(Integer.toString(mp.get(val)));

                            } else {

                                //Update the predecessor and partition the array to update the predecessor.

                                String tmp = myPredecessor;
                                ArrayList<String> keyList = new ArrayList<String>();
                                ArrayList<String> valList = new ArrayList<String>();
                                myPredecessor = genHash(Integer.toString(mp.get(val)));
                                myPredecessorPort = Integer.toString(val);
                                //Partition the hash table with respect to predecessor node.
                                Log.i(TAGS, "db size " + db.size());
                                if (!db.isEmpty()) {

                                    for (String key : db.keySet()) {
                                        if (key.compareTo(myPredecessor) <= 0) { //Collect the elements which belong to my predecessor from my hash table
                                            valList.add(db.get(key));
                                            keyList.add(key);
                                        }

                                    }

                                    //Removing the partitioned elements which belong to predecessor node
                                    for (int i = 0; i < keyList.size(); i++) {
                                        db.remove(keyList.get(i));
                                    }

                                    String req = MsgType.UPDATE_TABLE.getValue() + "," + valList.size() + ",";

                                    for (int i = 0; i < valList.size(); i++) {
                                        req = req + keyList.get(i) + "-" + valList.get(i) + ":";
                                    }

                                    req = req.substring(0, req.length() - 1);
                                    Log.i(TAGS, "Sending the partitioned hashtable: " + req);

                                    publishProgress(Integer.toString(MsgType.UPDATE_TABLE.getValue()), req, portMap.get(myPredecessor));
                                }
                            }


                        } else if (msgType == MsgType.UPDATE_TABLE.getValue()) {


                            os.writeUTF("ACK");
                            //int numRecords = Integer.parseInt(tokens[1]);
                            String st = tokens[2];
                            String[] toks = st.split(":");
                            for (String s : toks) {
                                Log.i(TAGS, "Process KeyValue pair: " + s);
                                String[] toke = s.split("-");

                                String key = toke[0];
                                db.put(key, toke[1]);
                            }
                        }


                        else if (msgType == MsgType.INSERT.getValue()) {


                            os.writeUTF("ACK");
                            String st = tokens[1];
                            String[] toks = st.split("-");
                            String key = toks[0];
                            String value = toks[1];

                            String temp = genHash(Integer.toString(mp.get(masterNode)));
                            Log.i(TAG,"masterNode: "+nodeHash.get(temp)+", temp: "+temp+", myID: "+myID+" myPred: "+myPredecessor+", mySucc: "+mySuccessor);

                            boolean tmp1 = myID.equals(temp);
                            int tmp2 = key.compareTo(myPredecessor);

                            Log.i(TAG," mNode equality: "+tmp1+", keyCompPred: "+tmp2);
                            String hashKey = genHash(key);
/*
                            if(Integer.parseInt(myPort) == masterNode) {
                                if(hashKey.compareTo(myID) <=0 || hashKey.compareTo(lastNode) > 0) {
                                    db.put(key, value);
                                    Log.i(TAGS, "Insert msg being terminated at master node11");
                                }
                            }
*/
                            boolean acceptable = isAcceptable(hashKey);

                            if(acceptable) {
                                Log.i(TAGS,"Accepting value at current avd "+key);
                                db.put(key, value);
                                Log.i(TAGS, "INSERT msg : " + key + "->" + value);
                            }
                            else {
                                Log.i(TAGS, "Forwarding it to further successor "+mySuccessorPort + " myport: "+myPort+" hashKey: "+hashKey);
                                publishProgress(Integer.toString(MsgType.INSERT.getValue()), str, mySuccessorPort, myPort);
                            }


                        }


                        else if(msgType == MsgType.QUERY_RESP.getValue()) {
                            os.writeUTF("ACK");
                            String requester = tokens[1];
                            String selection = tokens[2];
                            String resp = tokens[3];

                            Log.i(TAGS,"QUERY_RESP received requester: "+requester+" myPort: "+myPort+" resp: "+resp);
                            if(myPort.equals(requester)) {

//                                quer.add(selection+"#"+resp);
                                quer.put(selection, resp);

                                //queries.put(selection, resp);

                                Log.i(TAGS,"Resp accepted in queue");
                            }

                        }

                        else if (msgType == MsgType.QUERY_REQ.getValue()) {
                            os.writeUTF("ACK");

                            //Query Request can be of 3 types
                            //Either * or key specific

                            //Structure of msg ->   QUERY_REQ, requesterPort, selection
                            String requester = tokens[1];
                            String selection = tokens[2];
                            String resp = null;
                            if(tokens.length > 3) {
                                resp = tokens[3];
                            }

                            Log.i(TAGS,"QUERY_REQ messaage received requester: "+requester+" selection: "+selection+" resp: "+resp);
                            Log.i(TAGS,"QUERY_REQ DB size# "+db.size());
                            boolean asReq = false;

                            if(selection.equals(ALL)) {
                                resp = resp +":";
                                //Append own's hash dump into the response and forward it to successor as REQ
                                asReq = true;
                                for(String key : db.keySet()) {

                                    resp = resp + key + "-"+ db.get(key)+":";

                                }
                                resp = resp.substring(0, resp.length()-1);

                            }
                            else {   //Key specific query
                                //Pick response from the own hashTable. if present forward it to successor
                                if(db.get(selection)!=null) {
                                      resp = selection + "-" + db.get(selection);
                                }
                                else {
                                    //forward the request to further successor.
                                    asReq = true;
                                }

                            }

                            Log.i(TAGS, "QUERY_REQ processing requester"+ requester +" mySucc: "+mySuccessorPort+" asReq: "+asReq);
                            if(requester.equals(mySuccessorPort) && asReq == true) {
                                //Send QUERY_RESP rather than QUERY_REQ

                                asReq = false;
                            }


                            if(asReq) {
                                Log.i(TAGS,"QUERY_REQ sending further msg as resp: "+resp);
                                publishProgress(Integer.toString(MsgType.QUERY_REQ.getValue()), resp, requester, selection, myPort);

                            }
                            else {
                                Log.i(TAGS,"QUERY_RESP sending further msg as resp: "+resp);
                                publishProgress(Integer.toString(MsgType.QUERY_RESP.getValue()), resp, requester, selection, myPort);
                            }


                        }
                        else if(msgType == MsgType.DELETE.getValue()) {
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

                        }



                    }

                    if(os!=null)  os.close();
                    if(is!=null) is.close();


                } catch (EOFException e) {

                } catch (Exception e) {
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

            else if(strings[0].equals(Integer.toString(MsgType.UPDATE_NODE.getValue()))) {

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.UPDATE_NODE.getValue()), strings[1], strings[2], myPort);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.UPDATE_NODE.getValue()), strings[3] ,strings[4], myPort);

            }
            else if(strings[0].equals(Integer.toString(MsgType.UPDATE_TABLE.getValue()))) {
                if(!db.isEmpty()) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.UPDATE_TABLE.getValue()), strings[1], strings[2], myPort);
                }
            }
            else if(strings[0].equals(Integer.toString(MsgType.INSERT.getValue()))) {

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.INSERT.getValue()), strings[1], strings[2], myPort);
            }
            else if(strings[0].equals(Integer.toString(MsgType.QUERY_REQ.getValue()))) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.QUERY_REQ.getValue()), strings[1], strings[2], strings[3], myPort);
            }
            else if(strings[0].equals(Integer.toString(MsgType.QUERY_RESP.getValue()))) {
                //Send the query response to requesting node
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.QUERY_RESP.getValue()), strings[1], strings[2], strings[3], myPort);

                /*
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.QUERY_RESP.getValue()), strings[1], strings[2], myPort);
                //Relay the query request to successor node if resp is not an ACK
                if(!strings[1].equals("ACK")) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.QUERY_REQ.getValue()), strings[1], strings[2], myPort);
                }
                */
            }
            else if(strings[0].equals(Integer.toString(MsgType.DELETE.getValue()))) {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(MsgType.DELETE.getValue()), strings[1], strings[2], myPort);
            }
            else {
                Log.i(TAGS,"Unknown type of request received");

//                Log.i("check", "onProgressUpdate");
//                if (strings[0] == null) {
//                    Log.e("i", "Null received");
//                    return;
//                }
//                String strReceived = strings[0].trim();
//                Log.i("i", "str received in onProgressUpdate " + strReceived);
//
//                ContentValues keyValueToInsert = new ContentValues();
//
//                try {
//                    keyValueToInsert.put("key", genHash(strReceived));
//                } catch (NoSuchAlgorithmException e) {
//                    e.printStackTrace();
//                }
//                keyValueToInsert.put("value", strReceived);
//
//                try {
//                    Uri newUri = getContext().getContentResolver().insert(
//                            myUri,    // assume we already created a Uri object with our provider URI
//                            keyValueToInsert
//                    );
//                    Log.v(TAG, "inserted into uri " + newUri);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                Log.i("sending", "final checkpoint");
            }

            return;
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
            String remotePort = null;
            String req = null;
            Socket socket = null;
            try {

                int msgToSend = Integer.parseInt(msgs[0]);

                if(msgToSend == MsgType.SIGN_IN_REQ.getValue()) {
                    Log.i(TAGC, "Check if matched or not" + masterNode);
                        remotePort = Integer.toString(masterNode);
                        req = Integer.toString(MsgType.SIGN_IN_REQ.getValue()) + "," + myPort+","+ myID;
                }
              //Update node notification message needs to be sent to the predecessor or successor
                else if(msgToSend == MsgType.UPDATE_NODE.getValue() ||
                        msgToSend == MsgType.UPDATE_TABLE.getValue()) {

                    req = msgs[1];
                    remotePort = msgs[2];
                    Log.i(TAGC,"Checkpoint2 req: "+ req+ " remotePort: "+remotePort);

                }
                else if(msgToSend == MsgType.INSERT.getValue()) {
                    req = msgs[1];
                    remotePort = msgs[2];
                    Log.i(TAGC,"MSGTYPE: "+ msgToSend+ " req: "+req + " to: "+remotePort);

                }
                else if(msgToSend == MsgType.QUERY_REQ.getValue()) {

                    req = MsgType.QUERY_REQ.getValue()+","+msgs[2] + ","+msgs[3] + ","+msgs[1];
                    remotePort = mySuccessorPort;

                }
                else if(msgToSend == MsgType.QUERY_RESP.getValue()) {
                    Log.i(TAG, "Sending QUERY_RESP message to "+msgs[2]);
                    req = MsgType.QUERY_RESP.getValue()+","+msgs[2]+","+msgs[3]+","+msgs[1];
                    //req = msgs[1];
                    remotePort = msgs[2];
                }
                else if(msgToSend == MsgType.DELETE.getValue()) {
                    req = msgs[1];
                    remotePort = msgs[2];
                    Log.i(TAGC,"Sending DELETE as "+req+" msg to "+remotePort);
                }


                Log.i(TAGC,"Client side remotePort: "+remotePort + " msgType: "+msgToSend);
                Log.i(TAGC,"msgTosend "+msgToSend);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                       Integer.parseInt(remotePort));

                Log.i(TAGC,"socket " +socket);
                DataInputStream is = new DataInputStream(socket.getInputStream());
                DataOutputStream os = new DataOutputStream(socket.getOutputStream());

                String reply = "NONE";

                /*
                if(msgToSend == MsgType.SIGN_IN_REQ.getValue()) {

                    while(true) {
                        socket.setSoTimeout(100);
                        try {
                            Log.i(TAGC, "Sending SIGN_IN_REQ");
                            os.writeUTF(req);
                            reply = is.readUTF();
                            if (!reply.equals("NONE"))
                                break;
                        }catch (Exception e) {
                            Log.e(TAGC,"Sleeping for 20 ms");
                            Thread.sleep(100);
                        }
                    }

                    if(!reply.equals("NONE")) {
                        String[] toks = reply.split(",");
                        Log.i(TAGC, "Updating the successor and predecessor from SIGN_IN_RESP");
                        myPredecessorPort = toks[1];
                        mySuccessorPort = toks[2];
                        myPredecessor = genHash(myPredecessorPort);
                        mySuccessor = genHash(mySuccessorPort);
                    }
                }

                else {
                */
                    Log.i(TAGC, "Before sending the msg " + msgToSend);
                    os.writeUTF(req);
                    Log.i(TAGC, "Sent the msg with msgType: " + msgToSend);
                    do {
                        Log.i(TAGC, "Awaiting response for " + msgToSend);
                        reply = is.readUTF();
                        Log.i(TAGC, "Reply rcvd as " + reply);
                    } while (reply.equals("NONE"));
               // }

                Log.i(TAGC,"Out of do while loop now for "+msgToSend);
                if(msgToSend == MsgType.SIGN_IN_REQ.getValue()) {
                    Log.i(TAGC, "SIGN_IN_RESP received as " + reply);
                    if (!reply.equals("NONE")) {
                        String[] toks = reply.split(",");

                        myPredecessorPort = toks[1];
                        mySuccessorPort = toks[2];

                        myPredecessor = genHash(Integer.toString(mp.get(Integer.parseInt(myPredecessorPort))));
                        mySuccessor = genHash(Integer.toString(mp.get(Integer.parseInt(mySuccessorPort))));
                        //myPredecessor = genHash(myPredecessorPort);
                        //mySuccessor = genHash(mySuccessorPort);
                        Log.i(TAGC, "Updating the successor and predecessor from SIGN_IN_RESP as P: "+myPredecessor+" S: "+mySuccessor + " pport: "+myPredecessorPort+" sport: "+mySuccessorPort);
                    }
                }
                else {
                    Log.i(TAGC,"ACK received for "+msgToSend);
                }

                if(os!=null)    os.close();
                if(is!=null)    is.close();
                socket.close();

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

}
