package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by sheng-yungcheng on 4/24/17.
 */

public class SQL_my_temp_db extends SQLiteOpenHelper {

    public static final String TABLE_NAME = "mytemptable";
    public static final String Global_TABLE_NAME = "Gtable";
    public static final String KEY = "key";
    public static final String VALUE = "value";
    private static final String DATABASE_NAME = "mytempdatabase.db";
    private static final String DATABASE_CREATE = "create table "+TABLE_NAME +" ( "+ KEY +" text not null, " +VALUE+" text not null "+");";
    private static final String DATABASE_CREATE_Gtable = "create table "+Global_TABLE_NAME +" ( "+ KEY +" text not null, " +VALUE+" text not null "+");";
    public SQL_my_temp_db(Context context){
        super(context,DATABASE_NAME,null,1);
    }
    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS "+TABLE_NAME);
        db.execSQL("DROP TABLE IF EXISTS "+Global_TABLE_NAME);
        db.execSQL(DATABASE_CREATE);
        db.execSQL(DATABASE_CREATE_Gtable);

    }
    /*
    public void creatnewtable(String tablename){
        SQLiteDatabase db=new SQLiteDatabase();
        db.execSQL("create table "+tablename +" ( "+ KEY +" text not null, " +VALUE+" text not null "+");");
    }*/

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

    }
}