package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final int REMOTE_PORTS[] = {11108, 11112, 11116, 11120, 11124};
    static int count = 0;
    //static final String myUri = "edu.buffalo.cse.cse486586.groupmessenger1.provider";
    static Uri myUri=null;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);
      /*  try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        */
        Log.i("1","check0");
        Log.i("i","checkpoint 0" + TAG);
        myUri = buildUri("content","edu.buffalo.cse.cse486586.groupmessenger1.provider");
        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        Log.i(TAG,"check point1");
        TelephonyManager tel = (TelephonyManager) this.getSystemService(TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.i("ini","portStr "+portStr + " myPort "+myPort);

        try {

            ServerSocket serverSock = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSock);
            Log.i(TAG,"check point3");
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "Error creating Server Socket");
            return;
        }


        final EditText edTxt = (EditText) findViewById(R.id.editText1);
        final Button send = (Button) findViewById(R.id.button4);
/*
        edTxt.setOnKeyListener(new View.OnKeyListener() {
            @Override
            public boolean onKey(View v, int keyCode, KeyEvent event) {
                if ((event.getAction() == KeyEvent.ACTION_DOWN) &&
                        (keyCode == KeyEvent.KEYCODE_ENTER)) {

                    Log.i("editText","function call triggered");
                    String msg = edTxt.getText().toString() + "\n";
                    edTxt.setText("");
                    tv.append("\t" + msg);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                }
                    return false;
            }
        });
*/
        View.OnClickListener clickHandler = null;

        clickHandler = new View.OnClickListener() {
            public void onClick(View v) {

                Log.i("editText","button call triggered");
                String msg = edTxt.getText().toString() + "\n";
                edTxt.setText("");
                tv.append("\t" + msg);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

            }
        };
        send.setOnClickListener(clickHandler);
        Log.i("check","check again");
        Log.i(TAG,"check point4");




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
        ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

        Socket servSocket = null;

        do {

            try {
                servSocket = serverSocket.accept();
                BufferedReader input = null;
                InputStreamReader rdr  = new InputStreamReader(servSocket.getInputStream());
                input = new BufferedReader(rdr);
                OutputStreamWriter output = null;
                String str = null;
                String msgAck = "ACK";
                while((str = input.readLine())!=null) {
                    Log.e("i", "reading");

                    publishProgress(str);
                    Thread.sleep(40);
                    /*
                    output = new OutputStreamWriter(servSocket.getOutputStream());
                    //output.flush();
                    Log.e("i", "before sending");
                    output.write(msgAck, 0, msgAck.length());
                    Thread.sleep(10);
                    output.flush();

                    */
                    Log.e("i", "after publishProgress");
                }
            } catch (Exception e) {
                Log.e("error", "Caught exception 1");

                try {
                    serverSocket.close();
                    Log.e("error", "Caught exception 2");
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                e.printStackTrace();

            }


        }while(servSocket.isConnected());

        return null;

    }

    protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
        Log.i("check","onProgressUpdate");
        if(strings[0]==null) {
            Log.e("i","Null received");
            return;
        }
        String strReceived = strings[0].trim();
        Log.i("i","str received in onProgressUpdate " + strReceived);
        TextView remoteTextView = (TextView) findViewById(R.id.textView1);
        remoteTextView.append(strReceived + "\t\n");


        ContentValues keyValueToInsert = new ContentValues();
        keyValueToInsert.put("key", count);
        keyValueToInsert.put("value", strReceived);

        Log.i("sending","key "+ Integer.toString(count) + " value " + strReceived);
        count++;
        try {
            Uri newUri = getContentResolver().insert(
                    myUri,    // assume we already created a Uri object with our provider URI
                    keyValueToInsert
            );
            Log.v(TAG,"inserted into uri " + newUri);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        Log.i("sending","final checkpoint");


        return;
    }
}


private class ClientTask extends AsyncTask<String, Void, Void> {

    @Override
    protected Void doInBackground(String... msgs) {
        try {
            String remotePort = null;
            //REMOTE_PORT0;
            //if (msgs[1].equals(REMOTE_PORT0))
            //    remotePort = REMOTE_PORT1;
            for(int i=0;i<REMOTE_PORTS.length;i++) {
                remotePort = Integer.toString(REMOTE_PORTS[i]);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));

                OutputStreamWriter output = null;
                String msgToSend = msgs[0];
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */

                try {
                    output = new OutputStreamWriter(socket.getOutputStream());
                    Log.e("i", "before sending");
                    output.write(msgToSend, 0, msgToSend.length());
                    Log.e("i", "after sending " + msgToSend);

                    Thread.sleep(40);
                    output.flush();

                    /*
                    InputStreamReader rdr  = new InputStreamReader(socket.getInputStream());
                    BufferedReader input = null;
                    input = new BufferedReader(rdr);
                    String str = null;
                    Thread.sleep(10);
                    output.flush();
                    while((str = input.readLine())!=null) {
                        Log.v("ACK","Received ack as "+str);
                        if (str == "ACK") {

                            output.close();
                            socket.close();
                        }
                    }
                    */
                } catch (Exception e) {
                    e.printStackTrace();
                }


                output.close();
                socket.close();
            }
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException");
        }

        return null;
        }

    }

}