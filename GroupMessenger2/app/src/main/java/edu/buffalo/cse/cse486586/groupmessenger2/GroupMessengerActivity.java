package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.FloatMath;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import static android.R.id.content;
import static android.R.id.input;
import static java.util.Collections.max;


 class CustomComparator implements Comparator<Element> {
    @Override
    public int compare(Element o1, Element o2) {
        Float v1 = Float.parseFloat(o1.getSeq());
        Float v2 = Float.parseFloat(o2.getSeq());

        int ret = Float.compare(v1, v2);
        if(ret == 0) {

            //To resolve the conflict in sequences by using port number as PID

            v1 = v1 + o1.getPID()/100000;
            v2 = v2 + o2.getPID()/100000;

            o1.setSeq(Float.toString(v1));
            o2.setSeq(Float.toString(v2));


        }

        return ret;

    }
}

   class DCustomComparator implements Comparator<Element> {
    @Override
    public int compare(Element o1, Element o2) {
        Float v1 = Float.parseFloat(o1.getSeq());
        Float v2 = Float.parseFloat(o2.getSeq());

        int ret = Float.compare(v2, v1);
        return ret;

        }

    }

public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final int REMOTE_PORTS[] = {11108, 11112, 11116, 11120, 11124};

    static int count = 0;
    static int ct = 0;
    static int nFailed = 0;
    String agreedSeq = null;

    int MSGTYPE = 0;
    int SEQUENCE = 1;

    int ISAGREED =2;
    int ISPROPOSAL = 2;
    int PID = 3;
    int MSG = 4;

    static int failedPort = 0;

    Map<Integer, Integer> mp = new HashMap<Integer, Integer>();

    int PING_TEST = 4;

    private int PROPOSAL_REQUEST = 1;
    private int PROPOSAL_RESPONSE = 2;
    private int AGREEMENT = 3;

    private int FAILURE_BROADCAST = 4;

    static Comparator<Element> comp = new CustomComparator();
    static PriorityQueue<Element> pq = new PriorityQueue<Element>(10,comp);

    ArrayList<Element> deliveryQueue = new ArrayList<Element>();


    static Uri myUri=null;

    static Element toPublish;
    static String myPort = null;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        mp.put(11108, 5);
        mp.put(11112, 10);
        mp.put(11116, 15);
        mp.put(11120, 25);
        mp.put(11124, 30);


        myUri = buildUri("content","edu.buffalo.cse.cse486586.groupmessenger2.provider");

        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());


        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));


        TelephonyManager tel = (TelephonyManager) this.getSystemService(TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.i(TAG,"Checkpoint1 " + myPort);

        try {

            ServerSocket serverSock = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSock);

        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "Error creating Server Socket");
            return;
        }


        final EditText edTxt = (EditText) findViewById(R.id.editText1);
        final Button send = (Button) findViewById(R.id.button4);

        View.OnClickListener clickHandler = null;

        clickHandler = new View.OnClickListener() {
            public void onClick(View v) {

                String msg = edTxt.getText().toString() + "\n";
                edTxt.setText("");
                tv.append("\t" + msg);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

            }
        };

        send.setOnClickListener(clickHandler);
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            String TAGS = "SERVER -> ";

            ServerSocket serverSocket = sockets[0];

            Socket servSocket = null;

            do {

                try {


                    servSocket = serverSocket.accept();

                    DataInputStream is = new DataInputStream(servSocket.getInputStream());
                    DataOutputStream os = new DataOutputStream(servSocket.getOutputStream());

                    String str = null;

                    while(null!=(str = is.readUTF())) {
                       // Log.i(TAGS,"Msg received");
                        /*  At server 2 types of message would be received.
                        *
                        *   1. PROPOSAL_REQUEST message
                        *          For this we will check our count and reply with count+1 and add to the priority queue message details and mark it as undeliverable
                        *
                        *   2. AGREEMENT message
                        *           For this we simply update the corresponding message in priority queue with agreed sequence number and change the status to deliverable
                        *
                        *
                        * */

                        //check type of message - 1,2,3

                        String[] tokens = str.split(",");
                        int msgType = 0;
                        msgType = Integer.parseInt(tokens[MSGTYPE]);
                        Log.i(TAGS,"MSG RCVD "+str);

                        if (msgType == PROPOSAL_REQUEST) {

                            Element ele = new Element();
                            ele.setMsg(tokens[MSG]);
                            ele.setPID(Integer.parseInt(tokens[PID]));
                            ele.setisAgreed(Boolean.parseBoolean(tokens[ISAGREED]));
                            ele.setisDeliverable(false);

                            /*
                            * Add to the priority queue with details about the message and mark it as undeliverable
                            */


                            String reply = null;
                            count++;
                            float subSeq = Float.parseFloat(myPort)/100000;
                            float responseSeq = count + subSeq;

                            ele.setSeq(Float.toString(responseSeq));
                            reply = PROPOSAL_RESPONSE + "," + responseSeq + "," + "true," + myPort+","+ele.getMsg();
                            pq.add(ele);

                            os.writeUTF(reply);

                        } else if (msgType == AGREEMENT || msgType == FAILURE_BROADCAST) {

                            /*
                            *
                            * Update the priority queue with the updated sequence number and mark it as deliverable
                            *
                            */

                            String ack = "ACK";
                            if(FAILURE_BROADCAST == msgType) {
                                failedPort = Integer.parseInt(tokens[1]);
                                os.writeUTF(ack);
                            }
                            else {
                                Element ele = new Element();

                                os.writeUTF(ack);

                                float pid = Float.parseFloat(tokens[PID]);
                                ele.setPID(pid);

                                int pID = (int) pid;
                                float v = Float.parseFloat(tokens[SEQUENCE]) + (float) mp.get(pID) / 100000;

                                ele.setSeq(Float.toString(v));

                                int temp = (int) Math.ceil(Float.parseFloat(ele.getSeq()));
                                if (temp > count) {
                                    count = temp;
                                }
                                ele.setAgreedSeq(ele.getSeq());

                                ele.setMsg(tokens[MSG]);

                                ele.setisAgreed(Boolean.parseBoolean(tokens[AGREEMENT]));
                                ele.setisDeliverable(true);

                                Element tmp = null;
                                Element newElement = null;

                                for (Element e : pq) {

                                    if (e.getMsg().equals(ele.getMsg())) {

                                        newElement = new Element();
                                        newElement.setMsg(ele.getMsg());
                                        newElement.setSeq(ele.getAgreedSeq());
                                        newElement.setPID(ele.getPID());
                                        newElement.setisDeliverable(true);
                                        newElement.setAgreedSeq(ele.getAgreedSeq());
                                        tmp = e;
                                    }
                                }


                                pq.remove(tmp);
                                pq.add(newElement);

                                Log.i(TAGS, pq.toString());


                            }
                            Log.i(TAGS,"Wait over for all the messages failedPort : "+failedPort);

                            ArrayList<Element> ll = new ArrayList<Element>();
                            for(Element e : pq) {

                                int fP = (int) e.getPID();

                                if(e.isDeliverable()==false && fP==failedPort) {
                                    ll.add(e);
                                }
                            }


                            for(int k=0;k<ll.size();k++) {
                                Log.i(TAGS,"Removing msg as avd is no more alive : "+ll.get(k).getMsg());
                                pq.remove(ll.get(k));
                            }



                                while(pq.peek()!=null && pq.peek().isDeliverable()==true) {
                                    Log.i(TAGS,"Adding to delivery queue");
                                    deliveryQueue.add(pq.poll());
                                }


                            checkDeliveryQueue();
                            deliveryQueue.clear();
                        }

                    }

                    if(os!=null)  os.close();
                    if(is!=null) is.close();

                } catch (EOFException e) {

                }
                    catch (Exception e) {
                    e.printStackTrace();
                    try {
                        servSocket.close();
                    } catch (IOException e1) {
                    }


                }

            }while(servSocket.isConnected());

            return null;

        }


        void checkDeliveryQueue() {

            String TAGD = "DELIVERYCHECK -> ";
            Element ch =null;

            for(int i=0;i<deliveryQueue.size();i++) {


                ch = deliveryQueue.get(i);
                toPublish = ch;
                Log.i(TAGD,"deliver with seq "+ch.getSeq() + " msg < "+ ch.getMsg());
                publishProgress(ch.getMsg());

            }

        }


        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String TAGS = "SERVER -> ";

            if(strings[0]==null) {
                Log.e("i","Null received");
                return;
            }
            String strReceived = strings[0].trim();
            Log.i("i","str received in onProgressUpdate " + strReceived);
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");


            ContentValues keyValueToInsert = new ContentValues();

            keyValueToInsert.put("key", Integer.toString(ct));
            keyValueToInsert.put("value", strReceived);
            ct++;
            Log.i(TAGS, "key "+ Integer.toString(ct) + " value " + strReceived);

            try {
                Uri newUri = getContentResolver().insert(
                        myUri,    // assume we already created a Uri object with our provider URI
                        keyValueToInsert
                );
                Log.v(TAGS,"inserted into uri " + newUri);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            Log.i("sending","final checkpoint");

            toPublish = null;
            return;
        }
    }



    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String TAGC = "CLIENT -> ";

            try {
                String remotePort = null;

                /*
                * 1. First lets get an agreed sequence number
                * 2. Send the message with agreed sequence number
                * */

                String msgToSend = msgs[0];

                PriorityQueue<Element> arr = new PriorityQueue<Element>(10, new DCustomComparator());
                count++;

                for(int i=0;i<REMOTE_PORTS.length;i++) {
                    if(failedPort == REMOTE_PORTS[i]) {
                        continue;
                    }

                    remotePort = Integer.toString(REMOTE_PORTS[i]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(remotePort));
                    socket.setSoTimeout(500);

                    DataInputStream is = new DataInputStream(socket.getInputStream());
                    DataOutputStream os = new DataOutputStream(socket.getOutputStream());

                    String req = PROPOSAL_REQUEST + "," +count+ "," + "false," + myPort+ "," + msgToSend;
                    String reply = "NONE";

                    try {
                        os.writeUTF(req);

                        do {

                                reply = is.readUTF();


                            Log.i(TAGC, "Propose Response received from " + REMOTE_PORTS[i]+" : " + reply);

                        } while (reply.equals("NONE"));
                    }
                    catch(Exception e) {
                        Log.i(TAGC," Exception rcvd " + e);
                        Log.e(TAGC, "Timed out so skipping for port " + REMOTE_PORTS[i]);

                        failedPort = REMOTE_PORTS[i];
                        continue;
                    }

                    if(reply.equals("NONE")) {
                        Log.i(TAGC,"Failed port detected "+REMOTE_PORTS[i]);
                        failedPort = REMOTE_PORTS[i];
                    }
                    if(!reply.equals("NONE")) {

                        String[] tokens = reply.split(",");
                        Element ele = new Element();
                        ele.setSeq(tokens[SEQUENCE]);
                        ele.setMsg(tokens[MSG]);
                        float pid = Float.parseFloat(tokens[PID]);
                        pid = pid / 100000;
                        ele.setPID(pid);
                        ele.setisAgreed(Boolean.parseBoolean(tokens[ISAGREED]));

                        arr.add(ele);
                    }
                    socket.close();
                }


                Log.i(TAGC,"Proposals from rest are ");

                Log.i(TAGC,arr.toString());

                float mVal = Float.parseFloat(arr.peek().getSeq());
                arr.clear();

                Log.i(TAGC, "Received all awaited responses and agreedSeq "+mVal);

                String seq = Float.toString(mVal);
                for(int i=0;i<REMOTE_PORTS.length;i++) {

                    if(failedPort == REMOTE_PORTS[i]) {
                        continue;
                    }

                    remotePort = Integer.toString(REMOTE_PORTS[i]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(remotePort));

                    DataInputStream is = new DataInputStream(socket.getInputStream());
                    DataOutputStream os = new DataOutputStream(socket.getOutputStream());

                    String req = AGREEMENT + "," + seq + "," + "true," + myPort + "," + msgToSend;
                    String reply = "NONE";
                    try {
                        Log.i(TAGC,"Sending Agreed Sequenced msg to " + REMOTE_PORTS[i]);
                        os.writeUTF(req);

                        do {
                            reply = is.readUTF();
                        } while (!reply.equals("ACK"));
                    }catch (SocketTimeoutException e) {
                        Log.i(TAGC," Exception rcvd while sending AgreedSeq to "+REMOTE_PORTS[i]+" as " + e);

                        failedPort = REMOTE_PORTS[i];
                        nFailed = 1;
                        continue;
                    } catch (Exception e) {

                    }

                    if(!reply.equals("ACK")) {

                        failedPort = REMOTE_PORTS[i];
                        Log.i(TAGC," reply non-ack so update failedport"+failedPort);
                    }


                    os.flush();
                    if(os!=null) os.close();
                    if(is!=null) is.close();

                    socket.close();

                }


                /************************
                 *
                 *
                 *   Try gossiping protocol to make sure every avd comes to know about the failed port
                 *
                 */


              if(failedPort !=0) {

                  for (int i = 0; i < REMOTE_PORTS.length; i++) {

                      if (failedPort == REMOTE_PORTS[i]) {
                          continue;
                      }

                      remotePort = Integer.toString(REMOTE_PORTS[i]);
                      Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

                      DataInputStream is = new DataInputStream(socket.getInputStream());
                      DataOutputStream os = new DataOutputStream(socket.getOutputStream());

                      String req = FAILURE_BROADCAST + "," + failedPort;
                      String reply = "NONE";
                      try {
                          Log.i(TAGC, "Sending Failure Detection msg to " + REMOTE_PORTS[i]);
                          os.writeUTF(req);

                          do {
                              reply = is.readUTF();

                          } while (!reply.equals("ACK"));
                      } catch (SocketTimeoutException e) {

                          Log.i(TAGC, " Exception rcvd while sending failure broadcast to " + REMOTE_PORTS[i] + " as " + e);

                          failedPort = REMOTE_PORTS[i];
                          continue;
                      } catch (Exception e) {

                      }

                      if (!reply.equals("ACK")) {

                          failedPort = REMOTE_PORTS[i];
                          Log.i(TAGC, " reply non-ack so update failedport" + failedPort);
                      }


                      os.flush();
                      if (os != null) os.close();
                      if (is != null) is.close();

                      socket.close();

                  }
              }

            } catch (UnknownHostException e) {
                Log.e(TAGC, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAGC, "ClientTask socket IOException");
            } catch(NullPointerException e) {
                //e.printStackTrace();
            } catch (Exception e) {
                //e.printStackTrace();

            }

            return null;
        }

    }

}
