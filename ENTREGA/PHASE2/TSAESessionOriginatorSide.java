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
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

import recipes_service.ServerData;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.Operation;
import recipes_service.tsae.data_structures.Timestamp;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;
import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques
 *         December 2012
 *
 */
public class TSAESessionOriginatorSide extends TimerTask {
	private static AtomicInteger session_number = new AtomicInteger(0);

	private ServerData serverData;

	private final Object sessionLock = new Object();

	public TSAESessionOriginatorSide(ServerData serverData) {
		super();
		this.serverData = serverData;
	}

	/**
	 * Implementation of the TimeStamped Anti-Entropy protocol
	 */
	public void run() {
		sessionWithN(serverData.getNumberSessions());
	}

	/**
	 * This method performs num TSAE sessions
	 * with num random servers
	 * 
	 * @param num
	 */
	public void sessionWithN(int num) {
		if (!SimulationData.getInstance().isConnected())
			return;
		List<Host> partnersTSAEsession = serverData.getRandomPartners(num);
		Host n;
		for (int i = 0; i < partnersTSAEsession.size(); i++) {
			n = partnersTSAEsession.get(i);
			sessionTSAE(n);
		}
	}

	/**
	 * This method perform a TSAE session
	 * with the partner server n
	 * 
	 * @param n
	 */
	private void sessionTSAE(Host n) {
		int current_session_number = session_number.incrementAndGet();
		if (n == null) return;

		//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] TSAE session");

		Socket socket = null;
		synchronized (sessionLock) {
			try {
				socket = new Socket(n.getAddress(), n.getPort());
				ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());
				ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());

				TimestampVector localSummary = serverData.getSummary().clone();
				serverData.getAck().update(serverData.getId(), localSummary);
				TimestampMatrix localAck = serverData.getAck().clone();

				// Enviar solicitud AE
				MessageAErequest requestMsg = new MessageAErequest(localSummary, localAck);
				requestMsg.setSessionNumber(current_session_number);
				out.writeObject(requestMsg);
				//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] sent message: " + requestMsg);

				// Procesar mensajes de entrada
				Message msg = (Message) in.readObject();
				while (msg != null && msg.type() == MsgType.OPERATION) {
					LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] sent message: " + msg);
					Operation operation = ((MessageOperation) msg).getOperation();
					serverData.getLog().add(operation);
					serverData.execOperation(operation);
					serverData.getSummary().updateTimestamp(operation.getTimestamp());
					serverData.getAck().update(serverData.getId(), serverData.getSummary());
					msg = (Message) in.readObject();
				}

				if (msg != null && msg.type() == MsgType.AE_REQUEST) {
					TimestampVector partnerSummary = ((MessageAErequest) msg).getSummary();
					TimestampMatrix partnerAck = ((MessageAErequest) msg).getAck();

					List<Operation> newOperations = serverData.getLog().listNewer(partnerSummary);
					serverData.getAck().updateMax(partnerAck);

					if (newOperations != null) {
						for (Operation operation : newOperations) {
							MessageOperation operationMsg = new MessageOperation(operation);
							operationMsg.setSessionNumber(current_session_number);
							out.writeObject(operationMsg);
						}
					}

					// Enviar END_TSAE
					MessageEndTSAE endMsg = new MessageEndTSAE();
					endMsg.setSessionNumber(current_session_number);
					out.writeObject(endMsg);

					// Verificar END_TSAE
					msg = (Message) in.readObject();
					if (msg != null && msg.type() == MsgType.END_TSAE) {
						//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] TSAE session ended");
						serverData.getSummary().updateMax(partnerSummary);
						serverData.getAck().updateMax(partnerAck);
						serverData.getAck().update(serverData.getId(), serverData.getSummary());
					}
				} else {
					LSimLogger.log(Level.WARN, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] Invalid or null AE_REQUEST message received.");
				}
			} catch (ClassNotFoundException e) {
				LSimLogger.log(Level.WARN, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] ClassNotFound: " + e.getMessage());
			} catch (IOException e) {
				LSimLogger.log(Level.WARN, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] IOException: " + e.getMessage());
			} finally {
				try {
					socket.close();
				} catch (IOException e) {
					LSimLogger.log(Level.WARN, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] Error closing socket: " + e.getMessage());
				}
				//LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] End TSAE session");
			}
		}
	}
}