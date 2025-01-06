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
     * @param n
     */
    private void sessionTSAE(Host n) {
        int current_session_number = session_number.incrementAndGet();
        if (n == null)
            return;

        LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] TSAE session");

        try {
            Socket socket = new Socket(n.getAddress(), n.getPort());
            ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());
            ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());

            // Send to partner: local's summary and ack
            TimestampVector localSummary;
            TimestampMatrix localAck;
            synchronized (serverData) {
                localSummary = serverData.getSummary().clone();
                localAck = serverData.getAck().clone();
            }
            Message msg = new MessageAErequest(localSummary, localAck);
            msg.setSessionNumber(current_session_number);
            out.writeObject(msg);
            LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] sent message: " + msg);

            // Receive operations from partner
            msg = (Message) in.readObject();
            LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] received message: " + msg);
            while (msg.type() == MsgType.OPERATION) {
                MessageOperation operationMsg = (MessageOperation) msg;
                Operation operation = operationMsg.getOperation();
                synchronized (serverData) {
                    serverData.getLog().add(operation);
                }
                msg = (Message) in.readObject();
                LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] received message: " + msg);
            }

            // Receive partner's summary and ack
            if (msg.type() == MsgType.AE_REQUEST) {
                MessageAErequest partner = (MessageAErequest) msg;

                // Get operations newer than partner's summary
                List<Operation> newOperations;
                synchronized (serverData) {
                    newOperations = serverData.getLog().listNewer(partner.getSummary());
                }

                // Send operations to partner
                if (newOperations != null) {
                    for (Operation operation : newOperations) {
                        MessageOperation operationMsg = new MessageOperation(operation);
                        operationMsg.setSessionNumber(current_session_number);
                        out.writeObject(operationMsg);
                    }
                }

                // Send end of TSAE session message
                msg = new MessageEndTSAE();
                msg.setSessionNumber(current_session_number);
                out.writeObject(msg);
                LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] sent message: " + msg);

                // Receive end of session confirmation
                msg = (Message) in.readObject();
                LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] received message: " + msg);
                if (msg.type() == MsgType.END_TSAE) {
                    synchronized (serverData) {
                        // Update summary vector with partner's summary
                        serverData.getSummary().updateMax(partner.getSummary());
                        // Update ack matrix with partner's ack
                        serverData.getAck().updateMax(partner.getAck());
                        // Purge log
                        serverData.getLog().purgeLog(serverData.getAck());
                    }
                }
            }
            socket.close();
        } catch (ClassNotFoundException e) {
            LSimLogger.log(Level.FATAL, "[TSAESessionOriginatorSide] [session: " + current_session_number + "]" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            LSimLogger.log(Level.WARN, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] IOException: " + e.getMessage());
        }

        LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: " + current_session_number + "] End TSAE session");
    }
}