package edu.buffalo.cse.cse486586.groupmessenger1;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.util.Log;
import android.widget.TextView;

import org.w3c.dom.Text;

import java.util.HashMap;

/**
 * GroupMessengerProvider is a key-value table. Once again, please note that we do not implement
 * full support for SQL as a usual ContentProvider does. We re-purpose ContentProvider's interface
 * to use it as a key-value table.
 * 
 * Please read:
 * 
 * http://developer.android.com/guide/topics/providers/content-providers.html
 * http://developer.android.com/reference/android/content/ContentProvider.html
 * 
 * before you start to get yourself familiarized with ContentProvider.
 * 
 * There are two methods you need to implement---insert() and query(). Others are optional and
 * will not be tested.
 * 
 * @author stevko
 *
 */
public class GroupMessengerProvider extends ContentProvider {

    //private SQLiteDatabase db;
    //private final String TABLE_NAME = "MY_TABLE";
    //private int count = 0;

    private HashMap<String, String> db;

    //private String msg = null;
    //private String cntUri = "edu.buffalo.cse.cse486586.groupmessenger1.provider";
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        db.clear();
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // You do not need to implement this.
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        /*
         * TODO: You need to implement this method. Note that values will have two columns (a key
         * column and a value column) and one row that contains the actual (key, value) pair to be
         * inserted.
         * 
         * For actual storage, you can use any option. If you know how to use SQL, then you can use
         * SQLite. But this is not a requirement. You can use other storage options, such as the
         * internal storage option that we used in PA1. If you want to use that option, please
         * take a look at the code for PA1.
         */


        //db.execSQL("INSERT INTO" + TABLE_NAME + "(Key, Val)" + "VALUES (" + Integer.toString(count) + "," + values + ")";
        /*
        values.put("key",count);
        db.insert(TABLE_NAME, null, values);
        count++;
        */

        Log.i("insert","going to insert ");
        Log.i("insert","values "+ values.get("key") + ", " + values.get("value"));
        if(db.containsKey(values.get("key"))) {
            Log.i("insert","key already present hence removing " + values.get("key"));
            db.remove(values.get("key"));
        }
        db.put(values.get("key").toString(), values.get("value").toString());
        Log.v("insert","insertion done");
        //Log.v("insert", values.toString() + " seq " + Integer.toString(count-1));
        return uri;
    }

    @Override
    public boolean onCreate() {
        // If you need to perform any one-time initialization task, please do it here.
        /*
        Log.i("i","check before creating a table");
        //db.execSQL("CREATE TABLE" + TABLE_NAME + "(key varchar(255), value varchar(255))");
        db.execSQL("CREATE TABLE" + TABLE_NAME + "(key TEXT, value TEXT);");
        Log.i("i","table created with name " + TABLE_NAME);
        */

        db = new HashMap<String, String>();
        Log.i("create","creating a hashmap");
        return false;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // You do not need to implement this.
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        /*
         * TODO: You need to implement this method. Note that you need to return a Cursor object
         * with the right format. If the formatting is not correct, then it is not going to work.
         *
         * If you use SQLite, whatever is returned from SQLite is a Cursor object. However, you
         * still need to be careful because the formatting might still be incorrect.
         *
         * If you use a file storage option, then it is your job to build a Cursor * object. I
         * recommend building a MatrixCursor described at:
         * http://developer.android.com/reference/android/database/MatrixCursor.html
         */

        //Cursor curs = getContext().getContentResolver().query(uri, null, selection, null, null);
        /*
        Cursor curs = db.query(TABLE_NAME,null,selection,null,null,null,null);
        */

        MatrixCursor curs = new MatrixCursor(new String[]{"key", "value"});
        curs.moveToFirst();
        String k = selection;
        String v = db.get(k);
        curs.addRow(new Object[]{k, v});

        Log.d("query", "query key "+ selection + " value retrieved "+v);
        return curs;
    }
}
