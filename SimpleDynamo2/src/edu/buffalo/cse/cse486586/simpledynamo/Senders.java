package edu.buffalo.cse.cse486586.simpledynamo;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import android.os.AsyncTask;
import android.util.Log;

public class Senders {

	public void SendTask(Message message){
		Socket socket;
		for (int i=0;i<message.ConcernedNodes.length;i++){
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(message.ConcernedNodes[i]));
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(message);
				out.close();    
				socket.close(); 
				//	Log_dis("sent message", message.ConcernedNodes[i]);
			}
			catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public void reply_to_Query(Message message){

		for (int i=0; i<message.ConcernedNodes.length;i++){

			Wrapper wrap=new Wrapper();
			wrap.Receipient=message.ConcernedNodes[i];
			wrap.message=message;

			new SenderTask().runThread(wrap);
		}
	}	


	public void Log_dis(String Tag, String msg){
		Log.e(Tag,msg);
		if(Tag!=null && msg!=null){
		new Display().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Tag+msg, null);
		}
	}

	public class Display extends AsyncTask<String, String, Void>  {

		@Override
		protected Void doInBackground(String... wrap) {
			publishProgress(wrap[0]);
			return null;
		}


		public void onProgressUpdate(String...messages) {
			if(messages[0]!=null)
			{
			SimpleDynamoActivity.tv.append(messages[0]+"\n");
			}
		}
	}

	public class Wrapper{
		Message message;
		String Receipient;
	}

	public class SenderTask implements Runnable  {
		Wrapper wrap;

		public void runThread(Wrapper wrap){
			this.wrap=wrap;
			Thread thread1 = new Thread(this);
			thread1.start(); 	
		}

		@Override
		public void run() {

			for (int i=0;i<1;i++){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(wrap.Receipient));

					ObjectOutputStream out;	
					out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(wrap.message);
					Log.e("SenderTask"+"sent out the message "+wrap.Receipient,wrap.message.Values[0]);
					//Log.e(TAG, remotePort[i]);
					out.close();    
					socket.close();
					break;
				} 
				catch (UnknownHostException e) {
					Log.e("SenderTask", "ClientTask UnknownHostException");
				} 
				catch (IOException e) {
					Log.e("SenderTask socket IOException "+wrap.Receipient,wrap.message.Values[0]);

					//	Log.e("ClientTask socket IOException",e.getMessage());
				}	
			}					
		}
	}
}


