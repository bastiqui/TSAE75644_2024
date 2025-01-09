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
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import recipes_service.ServerData;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.Operation;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;
import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * Implementation of the TimeStamped Anti-Entropy protocol
 * with acknowledgments and log purging.
 * 
 * This class handles the originator side of the TSAE session.
 * It sends its local summary and ack to a partner and receives operations
 * that are newer than its local summary.
 * 
 * The session ends with an exchange of end messages.
 * 
 * Author: Joan-Manuel Marques
 * December 2012
 */
public class TSAESessionOriginatorSide extends TimerTask {
    private static AtomicInteger session_number = new AtomicInteger(0);

    private ServerData serverData;

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
        for (Host n : partnersTSAEsession) {
            sessionTSAE(n);
        }
    }

    /**
     * This method performs a TSAE session
     * with the partner server n
     * 
     * @param n the partner server to perform the session with
     */
    private void sessionTSAE(Host n) {
        // Increment the session number for the current session
        int current_session_number = session_number.incrementAndGet();
        if (n == null)
            return; // Exit if the partner server is null

        // Log the start of the TSAE session
        LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] TSAE session");

        try {
            // Establish a socket connection to the partner server
            Socket socket = new Socket(n.getAddress(), n.getPort());
            ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());
            ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());

            // Prepare and send the local summary and acknowledgment to the partner
            TimestampVector localSummary;
            TimestampMatrix localAck;
            synchronized (serverData) {
                localSummary = serverData.getSummary().clone(); // Clone the local summary
                localAck = serverData.getAck().clone(); // Clone the local acknowledgment
            }
            Message msg = new MessageAErequest(localSummary, localAck);
            msg.setSessionNumber(current_session_number);
            out.writeObject(msg); // Send the message to the partner
            LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] sent message: " + msg);

            // Receive operations from the partner
            msg = (Message) in.readObject();
            LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] received message: " + msg);
            while (msg.type() == MsgType.OPERATION) {
                // Process each operation received
                MessageOperation operationMsg = (MessageOperation) msg;
                Operation operation = operationMsg.getOperation();
                synchronized (serverData) {
                    serverData.execOperation(operation); // Use execOperation to handle both add and remove
                }
                msg = (Message) in.readObject(); // Read the next message
                LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] received message: " + msg);
            }

            // Check if the received message is a summary and acknowledgment request
            if (msg.type() == MsgType.AE_REQUEST) {
                MessageAErequest partner = (MessageAErequest) msg;

                // Retrieve operations that are newer than the partner's summary
                List<Operation> newOperations;
                synchronized (serverData) {
                    newOperations = serverData.getLog().listNewer(partner.getSummary());
                }

                // Send the newer operations to the partner
                if (newOperations != null) {
                    for (Operation operation : newOperations) {
                        MessageOperation operationMsg = new MessageOperation(operation);
                        operationMsg.setSessionNumber(current_session_number);
                        out.writeObject(operationMsg); // Send each operation
                        LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] sent operation: " + operation);
                    }
                }

                // Send an end of TSAE session message to the partner
                msg = new MessageEndTSAE();
                msg.setSessionNumber(current_session_number);
                out.writeObject(msg);
                LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] sent message: " + msg);

                // Receive confirmation of the end of the session from the partner
                msg = (Message) in.readObject();
                LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] received message: " + msg);
                if (msg.type() == MsgType.END_TSAE) {
                    synchronized (serverData) {
                        // Update the local summary and acknowledgment with the partner's data
                        serverData.getSummary().updateMax(partner.getSummary());
                        serverData.getAck().updateMax(partner.getAck());
                        // Purge the log of acknowledged operations
                        serverData.getLog().purgeLog(serverData.getAck());
                        LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] updated summary and ack");
                    }
                }
            }
            socket.close(); // Close the socket connection
        } catch (ClassNotFoundException e) {
            // Log and handle the exception if a class is not found during deserialization
            LSimLogger.log(Level.FATAL, "[TSAESessionOriginatorSide] [session: " + current_session_number + "]" + e.getMessage());
            e.printStackTrace();
            System.exit(1); // Exit the program on fatal error
        } catch (IOException e) {
            // Log a warning if an I/O exception occurs
            LSimLogger.log(Level.WARN, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] IOException: " + e.getMessage());
        }

        // Log the end of the TSAE session
        LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] End TSAE session");
    }
}