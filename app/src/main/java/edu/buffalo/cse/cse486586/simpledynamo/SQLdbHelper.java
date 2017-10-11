package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by sheng-yungcheng on 4/24/17.
 */

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class SQLdbHelper extends SQLiteOpenHelper {

    public static final String TABLE_NAME = "mytable";
    public static final String KEY = "key";
    public static final String VALUE = "value";
    private static final String DATABASE_NAME = "mydatabase.db";
    private static final String DATABASE_CREATE = "create table "+TABLE_NAME +" ( "+ KEY +" text not null, " +VALUE+" text not null "+");";
    public SQLdbHelper(Context context){
        super(context,DATABASE_NAME,null,1);
    }
    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS "+TABLE_NAME);
        db.execSQL(DATABASE_CREATE);

    }


    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }
}