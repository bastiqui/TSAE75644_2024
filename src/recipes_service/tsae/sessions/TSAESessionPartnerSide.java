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
 *         December 2012
 *
 */
public class TSAESessionPartnerSide extends Thread {

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

			synchronized (serverData) {
				// Clone the local summary and update the acknowledgment matrix
				localSummary = this.serverData.getSummary().clone();
				serverData.getAck().update(serverData.getId(), localSummary);
				localAck = this.serverData.getAck().clone();
			}

			// Receive request from originator and update local state
			// First, receive originator's summary and ack
			msg = (Message) in.readObject();
			current_session_number = msg.getSessionNumber();

			// Check if the message is an Anti Entropy Session Request
			if (msg.type() == MsgType.AE_REQUEST) {
				// Cast the message to MessageAErequest
				MessageAErequest originator = (MessageAErequest) msg;
				// Get operations that are newer than the originator's summary
				List<Operation> operations = serverData.getLog().listNewer(originator.getSummary());

				// Send operations to the originator
				for (Operation op : operations) {
					// Create a new operation message
					msg = new MessageOperation(op);
					// Set the session number for the message
					msg.setSessionNumber(current_session_number);
					// Send the message
					out.writeObject(msg);
				}

				// Send local's summary and ack to the originator
				msg = new MessageAErequest(localSummary, localAck);
				msg.setSessionNumber(current_session_number);
				out.writeObject(msg);

				// Receive operations from the originator
				List<Operation> ops = new ArrayList<Operation>(); // Create a list to store received operations
				msg = (Message) in.readObject();

				// Process each received operation
				while (msg.type() == MsgType.OPERATION) {
					// Extract the operation from the message
					Operation op = ((MessageOperation) msg).getOperation();
					// Add the operation to the list
					ops.add(op);
					// Read the next message
					msg = (Message) in.readObject();
				}

				// Check if the message indicates the end of the TSAE session
				if (msg.type() == MsgType.END_TSAE) {
					// Send an "end of TSAE session" message back to the originator
					msg = new MessageEndTSAE();
					msg.setSessionNumber(current_session_number);
					out.writeObject(msg);

					synchronized (serverData) {
						// Execute each received operation
						for (Operation op : ops) {
							serverData.execOperation(op);
							LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] executed operation: " + op);
						}
						// Update the local summary and acknowledgment matrix
						serverData.getSummary().updateMax(originator.getSummary());
						serverData.getAck().updateMax(originator.getAck());
						// Purge the log based on the updated acknowledgment matrix
						serverData.getLog().purgeLog(serverData.getAck());
						LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] updated summary and ack");
					}
				}
			}
			// Close the socket connection
			socket.close();
		} catch (ClassNotFoundException e) {
			// Handle exception for class not found
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			// Handle IO exception
		}
	}
}
