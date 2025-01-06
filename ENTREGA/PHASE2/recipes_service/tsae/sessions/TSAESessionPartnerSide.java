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
			// Initialize output and input streams for communication
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());

			// Synchronize to avoid interference between threads
			TimestampVector localSummary;
			TimestampMatrix localAck;
			
			synchronized(serverData){
				// Clone the local summary and update the acknowledgment matrix
				localSummary = this.serverData.getSummary().clone();
				serverData.getAck().update(serverData.getId(), localSummary);
				localAck = this.serverData.getAck().clone();
			}
			
			// Receive request from originator and update local state
			// First, receive originator's summary and acknowledgment
			msg = (Message) in.readObject();
			current_session_number = msg.getSessionNumber();
			
			if (msg.type() == MsgType.AE_REQUEST){
				// If AE_REQUEST (Anti Entropy Session Request message) is received
				MessageAErequest originator = (MessageAErequest) msg;
				// List operations that are newer than the originator's summary
				List<Operation> operations = serverData.getLog().listNewer(originator.getSummary());
	            
				// Send operations to the originator
				for (Operation op : operations){
					msg = new MessageOperation(op); // Create a new operation message
					msg.setSessionNumber(current_session_number); // Set session number
					out.writeObject(msg); // Send the message
				}

				// Send local's summary and acknowledgment to the originator
				msg = new MessageAErequest(localSummary, localAck); // Create a new request
				msg.setSessionNumber(current_session_number);
				out.writeObject(msg);

	            // Receive operations from the originator
				List<Operation> ops = new ArrayList<Operation>(); // Create a new list for operations
				msg = (Message) in.readObject();
				
				while (msg.type() == MsgType.OPERATION){
					// Add received operations to the list
					Operation op = ((MessageOperation) msg).getOperation(); 
					ops.add(op);
					msg = (Message) in.readObject();
				}
				
				// Receive message indicating the end of the TSAE session
				if (msg.type() == MsgType.END_TSAE){
					// Send an "end of TSAE session" message
					msg = new MessageEndTSAE();
					msg.setSessionNumber(current_session_number);
		            out.writeObject(msg);

					// Synchronize to avoid interference between threads
					synchronized(serverData){
						// Execute received operations
						for(Operation op : ops){
							serverData.execOperation(op);
						}
						// Update local summary and acknowledgment matrix with originator's data
						serverData.getSummary().updateMax(originator.getSummary());
						serverData.getAck().updateMax(originator.getAck());
						// serverData.getLog().purgeLog(serverData.getAck()); // To be added in phase 3: purge log
					}
				}	
			}
			// Close the socket after the session ends
			socket.close();		
		} catch (ClassNotFoundException e) {
			// Handle ClassNotFoundException
			e.printStackTrace();
            System.exit(1);
		} catch (IOException e) {
			// Handle IOException
		}
	}
}
