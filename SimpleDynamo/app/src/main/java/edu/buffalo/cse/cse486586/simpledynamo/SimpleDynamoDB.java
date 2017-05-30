package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.util.Log;

import java.io.File;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.concurrent.locks.ReentrantReadWriteLock.*;

/**
 * Created by andi on 4/26/17.
 */

public class SimpleDynamoDB extends SQLiteOpenHelper {


    private static final String DATABASE_NAME = "DynamoDB.db";
    private static final String TABLE_NAME = "dynamo";
    private static final String SQL_CREATE = "CREATE TABLE " + TABLE_NAME +
            " (key TEXT PRIMARY KEY, VALUE TEXT , VERSION BIGINT)";

    private static final String SQL_DROP = "DROP TABLE IS EXISTS '" + TABLE_NAME+"'" ;

    public SimpleDynamoDB(Context context) { //, String name, SQLiteDatabase.CursorFactory factory, int version) {
        super(context, DATABASE_NAME, null, 1);
    }

    public boolean doesExist() {
        Cursor cur = this.getReadableDatabase().rawQuery("select * from "+TABLE_NAME, null);
        boolean ret = (cur.getCount() > 0);
        return ret;

    }

    @Override
    public void onCreate(SQLiteDatabase db) {

        db.execSQL(SQL_CREATE);
        Log.i("i", "db created!!");

    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        this.deleteFromDB("*");
        db.execSQL(SQL_DROP);
        onCreate(db);
    }

    public Cursor getValues(String key, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        SQLiteQueryBuilder sqliteQueryBuilder = new SQLiteQueryBuilder();
        sqliteQueryBuilder.setTables(TABLE_NAME);

        if(key != null) {
            sqliteQueryBuilder.appendWhere("key" + " = " + key);
        }

        if(sortOrder == null || sortOrder=="") sortOrder = "VERSION";

        Cursor cursor = sqliteQueryBuilder.query(getReadableDatabase(),
                projection,
                selection,
                selectionArgs,
                null,
                null,
                sortOrder);
        return cursor;


    }

    public Cursor retrieveFromDB(String key){

        Log.i("i","retrievalFromDB "+key);
        Cursor ret = null;
        if(key.equals("*")) {
            ret = this.getReadableDatabase().rawQuery("select * from "+TABLE_NAME, null);
        }
        else {
            ret = this.getReadableDatabase().rawQuery("select * from " + TABLE_NAME + " where key = '" + key +"'", null);
        }
        Log.i("i","retrDB count" + ret.getCount());
        return ret;
    }

    public void insertToDB(String key, String value, long version) {

        String query = "SELECT * from "+TABLE_NAME+" where key='"+key+"'";
        Cursor curs = this.getReadableDatabase().rawQuery(query, null);
        long ver = 0;
        if(curs.getCount() > 0) {
            curs.moveToFirst();
            ver = curs.getLong(curs.getColumnIndex("VERSION"));
        }

        if(ver < version ) {
            Log.i("i", "insert start");
            ContentValues ob = new ContentValues();
            ob.put("key", key);
            ob.put("VALUE", value);
            ob.put("VERSION", version);
            //this.getWritableDatabase().update(TABLE_NAME,ob, "key='"+key+"'", null);
            this.getWritableDatabase().insertWithOnConflict(TABLE_NAME, null, ob, SQLiteDatabase.CONFLICT_REPLACE);

        }
        Log.i("i", "inserting into DB "+key+" - " + value + " -" + version + " ver: " + ver);
        Cursor c = this.getReadableDatabase().rawQuery("select * from "+TABLE_NAME+" where key ='"+key+"'",null);
        if(c.getCount()>0) {
            c.moveToFirst();
            Log.i("i", "cross check "+c.getString(c.getColumnIndex("VALUE")));
        }
        Log.i("i","bbye");

    }

    public void updateIfExists(String key, String value, long version) {
        String query = "SELECT * from "+TABLE_NAME+" where key='"+key+"'";
        Cursor curs = this.getReadableDatabase().rawQuery(query, null);
        long ver = 0;
        if(curs.getCount() > 0) {
            curs.moveToFirst();
            ver = curs.getLong(curs.getColumnIndex("VERSION"));
            if(ver < version) {
                ContentValues ob = new ContentValues();
                ob.put("key", key);
                ob.put("VALUE", value);
                ob.put("VERSION", version);
                this.getWritableDatabase().update(TABLE_NAME, ob, "key='"+key+"'",null);
                Log.i("i", "Updating the key "+key+" to value "+value);

            }

        }

        Log.i("i", "updating the DB "+key+" - " + value + " -" + version + " ver: " + ver);
        Cursor c = this.getReadableDatabase().rawQuery("select * from "+TABLE_NAME+" where key ='"+key+"'",null);
        if(c.getCount()>0) {
            c.moveToFirst();
            Log.i("i", "cross check "+c.getString(c.getColumnIndex("VALUE")));
        }
        Log.i("i","bbye");
    }

    public void deleteFromDB(String selection) {

        if(selection.equals("*") || selection.equals("@")) {
            this.getWritableDatabase().delete(TABLE_NAME, null, null);
            Log.i("i","cross verify delete *, @");
            this.retrieveFromDB(selection);
        }
        else {

            this.getWritableDatabase().delete(TABLE_NAME, "key = '" + selection + "'", null);
            Log.i("i","cross verify delete");
            this.retrieveFromDB(selection);
        }


    }
}
