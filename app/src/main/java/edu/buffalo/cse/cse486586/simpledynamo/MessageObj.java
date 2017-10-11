package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by sheng-yungcheng on 4/24/17.
 */

public class MessageObj implements Serializable {

    String messagetype;
    String fromID;
    String preID;
    String sucID;
    String msg_desti_port;
    String insertkey;
    String insertvalue;
    String selection;
    String queryresult;
    //String querykey;
    //ArrayList<Pair<String,String>> query_all_list=new ArrayList<Pair<String,String>>();
    HashMap<String,String> keyValueMap = new HashMap<String,String>();
    ArrayList<String> nodelist=new ArrayList<String>();
    ArrayList<String> nodelist_originalID=new ArrayList<String>();
    MessageObj(String msgtp,String frmid){
        this.messagetype=msgtp;
        this.fromID=frmid;

    }
    MessageObj(String msgtp,String frmid, String key,String value){
        this.messagetype=msgtp;
        this.fromID=frmid;
        this.insertkey=key;
        this.insertvalue=value;

    }

}

