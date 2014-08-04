package edu.buffalo.cse.cse486586.simpledynamo;

import java.

io.Serializable;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.LinkedList;

import android.util.Log;


public class CirLinkedList {

	public LinkedList<HashCompare> List= new LinkedList<HashCompare>();

	int getPosition(String Port){

		for(int i=0;i<List.size();i++){
			if(List.get(i).Port.equals(Port)){
				return i;
			}
		}
		return -1;
	}

	HashCompare getNext(int pos){
		if(pos+1<List.size()){
			return List.get(pos+1);
		}
		else{
			return List.get(0);
		}
	}

	HashCompare getNexttoNext(int pos){
		if(pos+2<List.size()){
			return List.get(pos+2);
		}
		else if (pos+1<List.size()){
			return List.get(0);
		}
		else{
			return List.get(1);
		}
	}

	HashCompare getPrev(int pos){
		if(pos-1>=0){
			return List.get(pos-1);
		}
		else {
			return List.get(List.size()-1);
		}
	}

	HashCompare get(int pos){
		if(pos>=0 && pos<List.size()){
			return List.get(pos);
		}
		else{
			return null;
		}
	}

	HashCompare getPrevtoPrev(int pos){
		if(pos-2>=0){
			return List.get(pos-2);
		}
		else if (pos-1>=0){
			return List.get(List.size()-1);
		}
		else{
			return List.get(List.size()-2);
		}
	}

	String [] getConcernedNodes(String keyHash){
		String [] Nodes= new String[4];
		int i;

		if(keyHash.compareTo(List.get(0).hash) < 0 || keyHash.compareTo(List.get(List.size()-1).hash) > 0){
			Nodes[0]=List.get(0).Port;
			Nodes[1]=getNext(0).Port;
			Nodes[2]=getNexttoNext(0).Port;
			//Log.e("key minNode "+keyHash+" ",Nodes[0]);
		}

		else {
			for (i=1;i<List.size();i++){
				if (keyHash.compareTo(List.get(i).hash) < 0 && keyHash.compareTo(List.get(i-1).hash) > 0){
					Nodes[0]=List.get(i).Port;
					Nodes[1]=getNext(i).Port;
					Nodes[2]=getNexttoNext(i).Port;
				//	Log.e("key to Node "+keyHash+" ",Nodes[0]);
				}
			}
		}
		return Nodes;
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

	void createList() throws NoSuchAlgorithmException{
		String remotePort []= {"11108","11112","11116","11120","11124"};

		for (int i=0;i<5;i++){
			HashCompare node = new HashCompare(genHash(String.valueOf(Integer.parseInt(remotePort[i])/2)),remotePort[i]);
			List.add(node);
		}
		Collections.sort(List);
	}
}

class ReplyArray implements Serializable{
	private static final long serialVersionUID = 1L;
	String Key;
	String Value;
	String Version;

}

class HashCompare implements Comparable <HashCompare>{

	public Socket socket;
	public String Port;
	public String hash;

	public HashCompare(String Hashval, String Port){		
		this.hash=Hashval;
		this.Port=Port;
	}

	@Override
	public int compareTo(HashCompare another) {
		// TODO Auto-generated method stub
		return this.hash.compareTo(another.hash);
	}		
}

class Message implements Serializable{
	private static final long serialVersionUID = 1L;
	String MessageType;
	String MsgInitiator;
	String [] Values;
	ArrayList <ReplyArray> Reply;
	String [] ConcernedNodes;
	int delete;
	int NoofNodes=1;

	public Message(String MessageType, String MsgInitiator, String [] Values, String [] ConcernedNodes){
		this.MessageType=MessageType;
		this.MsgInitiator=MsgInitiator;
		this.Values=Values;			
		this.ConcernedNodes=ConcernedNodes;
	}

	/*	public Message(String MessageType, String MsgInitiator, String [] Values, String [] Peers, int nodes){
		this.MessageType=MessageType;
		this.MsgInitiator=MsgInitiator;

		this.Values=Values;			
		this.Peers=Peers;
		this.NoofNodes=nodes;
	}*/
}
