/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.sessions;


import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Vector;

import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import recipes_service.ServerData;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.Operation;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;

import lsim.library.api.LSimLogger; //TODO check login system

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionPartnerSide extends Thread{

	private Socket socket = null;
	private ServerData serverData = null;

	public TSAESessionPartnerSide(Socket socket, ServerData serverData) {
		super("TSAEPartnerSideThread");
		this.socket = socket;
		this.serverData = serverData;
	}

	public void run() {

		Message msg = null;

		int current_session_number = -1;
		try {
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());

			//to synchronize for avoiding interference (Two actions issued by different threads may interleave)
			TimestampVector localSummary;
			TimestampMatrix localAck;

			synchronized(serverData){
				localSummary=this.serverData.getSummary().clone();
				serverData.getAck().update(serverData.getId(),localSummary);
				localAck=this.serverData.getAck().clone();
			}
			// Receive request from originator and update local state
			// First receive originator's summary and ack
			msg = (Message) in.readObject();
			current_session_number = msg.getSessionNumber();
			//LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] TSAE session");
			//LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
			if (msg.type() == MsgType.AE_REQUEST){
				// ... If AE_request (Anti Entropy Session Request message) DO
				MessageAErequest originator=(MessageAErequest)msg;
				List<Operation> operations=serverData.getLog().listNewer(originator.getSummary());
	            // send operations
				for (Operation op:operations){
					msg=new MessageOperation(op); //create new object (operation message)
					msg.setSessionNumber(current_session_number); //Exchange order with next one. First set sessionNumber
					out.writeObject(msg);
					//lsim.log(Level.TRACE,"[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);
				}

				// send to originator: local's summary and ack
				//Next 2 lines passed upstream before synchronize.
				//TimestampVector localSummary = null;
				//TimestampMatrix localAck = null;
				msg = new MessageAErequest(localSummary, localAck); //create new request
				msg.setSessionNumber(current_session_number);
				out.writeObject(msg);
				//lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);

	            // receive operations
				List<Operation> ops=new ArrayList<Operation>(); //create new object (array of operations)
				msg = (Message) in.readObject();
				//lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
				while (msg.type() == MsgType.OPERATION){
					Operation op=((MessageOperation)msg).getOperation();
					ops.add(op); 			//First add operation (addRecipe)
					msg = (Message) in.readObject();
					//lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
				}

				// receive message to inform about the ending of the TSAE session
				if (msg.type() == MsgType.END_TSAE){
					// send and "end of TSAE session" message
					msg = new MessageEndTSAE();
					msg.setSessionNumber(current_session_number);
					out.writeObject(msg);
					//lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);

					synchronized(serverData) { //to synchronize for avoiding interference (Two actions issued by different threads may interleave)
						for(Operation op : ops){
							serverData.execOperation(op);
						}
						serverData.getSummary().updateMax(originator.getSummary());
						serverData.getAck().updateMax(originator.getAck());
						serverData.getLog().purgeLog(serverData.getAck()); //TO ADD for phase 3: purge log
					}
				}
			}
			socket.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			//lsim.log(Level.FATAL, "[TSAESessionPartnerSide] [session: "+current_session_number+"]" + e.getMessage());
			e.printStackTrace();
            System.exit(1);
		}catch (IOException e) {
		}

		//lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] End TSAE session");
	}
}
