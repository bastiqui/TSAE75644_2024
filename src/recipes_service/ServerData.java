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

package recipes_service;

import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.Vector;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
import recipes_service.communication.Hosts;
import recipes_service.data.AddOperation;
import recipes_service.data.Operation;
import recipes_service.data.OperationType;
import recipes_service.data.Recipe;
import recipes_service.data.Recipes;
import recipes_service.data.RemoveOperation;
import recipes_service.tsae.data_structures.Log;
import recipes_service.tsae.data_structures.Timestamp;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;
import recipes_service.tsae.sessions.TSAESessionOriginatorSide;
/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class ServerData {

	// server id
	private String id;

	// sequence number of the last recipe timestamped by this server
	//private long seqnum=Timestamp.NULL_TIMESTAMP_SEQ_NUMBER; // sequence number (to timestamp)
	private AtomicLong seqnum = new AtomicLong(Timestamp.NULL_TIMESTAMP_SEQ_NUMBER);

	// timestamp lock
	//private final Object sessionLock = new Object();

	// TSAE data structures
	private Log log = null;
	private TimestampVector summary = null;
	private TimestampMatrix ack = null;

	// recipes data structure
	private Recipes recipes = new Recipes();

	// number of TSAE sessions
	int numSes = 1; // number of different partners that a server will contact for a TSAE session each time that TSAE timer (each sessionPeriod seconds) expires

	// propDegree: (default value: 0) number of TSAE sessions done each time a new data is created
	int propDegree = 0;

	// Participating nodes
	private Hosts participants;

	// TSAE timers
	private long sessionDelay;
	private long sessionPeriod = 10;

	private Timer tsaeSessionTimer;

	//
	TSAESessionOriginatorSide tsae = null;

	// TODO: esborrar aquesta estructura de dades
	// tombstones: timestamp of removed operations
	//List<Timestamp> tombstones = new Vector<Timestamp>();
	private final List<Timestamp> tombstones = new CopyOnWriteArrayList<>();

	// end: true when program should end; false otherwise
	private boolean end = false;

	public ServerData(){
	}

	/**
	 * Starts the execution
	 * @param participantss
	 */
	public void startTSAE(Hosts participants){
		this.participants = participants;
		this.log = new Log(participants.getIds());
		this.summary = new TimestampVector(participants.getIds());
		this.ack = new TimestampMatrix(participants.getIds());

		tsae = new TSAESessionOriginatorSide(this);
        tsaeSessionTimer = new Timer();
		tsaeSessionTimer.scheduleAtFixedRate(tsae, sessionDelay, sessionPeriod);
	}

	public void stopTSAEsessions(){
		if (tsaeSessionTimer != null) {
            tsaeSessionTimer.cancel();
        }
	}

	public boolean end(){
		return this.end;
	}

	public void setEnd(){
		this.end = true;
	}

	// ******************************
	// *** timestamps
	// ******************************
	private Timestamp nextTimestamp() {
        return new Timestamp(id, seqnum.incrementAndGet());
    }

	// ******************************
	// *** add and remove recipes
	// ******************************
	public void addRecipe(String recipeTitle, String recipe) {
		if (recipeTitle == null || recipe == null) {
			LSimLogger.log(Level.WARN, "Attempted to add a recipe with null values: title=" + recipeTitle + ", recipe=" + recipe);
			return;
		}

		Timestamp timestamp = nextTimestamp();
		Recipe rcpe = new Recipe(recipeTitle, recipe, id, timestamp);
		Operation op = new AddOperation(rcpe, timestamp);

		log.add(op);
		summary.updateTimestamp(timestamp);
		recipes.add(rcpe);

		LSimLogger.log(Level.INFO, String.format("Recipe added: Title='%s', Author='%s', Timestamp=%s", recipeTitle, id, timestamp));
	}

	public synchronized void removeRecipe(String recipeTitle) {
		Recipe removedRecipe = recipes.get(recipeTitle);
		if (removedRecipe != null) {
			Timestamp timestamp = nextTimestamp();
			RemoveOperation removeOp = new RemoveOperation(recipeTitle, removedRecipe.getTimestamp(), timestamp);
			log.add(removeOp);
			summary.updateTimestamp(timestamp);
			tombstones.add(removedRecipe.getTimestamp());
			recipes.remove(recipeTitle);
			LSimLogger.log(Level.INFO, "Recipe removed: " + recipeTitle);
		} else {
			LSimLogger.log(Level.WARN, "Attempted to remove non-existent recipe: " + recipeTitle);
		}
	}

	private synchronized void purgeTombstones() {
		if (ack == null) {
			LSimLogger.log(Level.WARN, "Attempted to purge tombstones with null ACK structure.");
			return;
		}

		TimestampVector minAckVector = ack.minTimestampVector();
		tombstones.removeIf(ts -> {
			Timestamp lastAck = minAckVector.getLast(ts.getHostid());
			return lastAck != null && ts.compare(lastAck) <= 0;
		});
	}


	// ****************************************************************************
	// *** operations to get the TSAE data structures. Used to send to evaluation
	// ****************************************************************************
	public Log getLog() {
		return log;
	}
	public TimestampVector getSummary() {
		return summary;
	}
	public TimestampMatrix getAck() {
		return ack;
	}
	public Recipes getRecipes(){
		return recipes;
	}

	// ******************************
	// *** getters and setters
	// ******************************
	public void setId(String id){
		this.id = id;
	}
	public String getId(){
		return this.id;
	}

	public int getNumberSessions(){
		return numSes;
	}

	public void setNumberSessions(int numSes){
		this.numSes = numSes;
	}

	public int getPropagationDegree(){
		return this.propDegree;
	}

	public void setPropagationDegree(int propDegree){
		this.propDegree = propDegree;
	}

	public void setSessionDelay(long sessionDelay) {
		this.sessionDelay = sessionDelay;
	}
	public void setSessionPeriod(long sessionPeriod) {
		this.sessionPeriod = sessionPeriod;
	}
	public TSAESessionOriginatorSide getTSAESessionOriginatorSide(){
		return this.tsae;
	}

	// ******************************
	// *** other
	// ******************************

	public List<Host> getRandomPartners(int num){
		return participants.getRandomPartners(num);
	}

	/**
	 * waits until the Server is ready to receive TSAE sessions from partner servers
	 */
	public synchronized void waitServerConnected(){
		while (!SimulationData.getInstance().isConnected()){
			try {
				wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				//			e.printStackTrace();
			}
		}
	}

	/**
	 * 	Once the server is connected notifies to ServerPartnerSide that it is ready
	 *  to receive TSAE sessions from partner servers
	 */
	public synchronized void notifyServerConnected(){
		notifyAll();
	}

	// *******************
	// *** PERS
	// *******************

	// Executes the given operation on the server data
	public void execOperation(Operation op) {
		// Check if the operation is null and log a warning if so
		if (op == null) {
			LSimLogger.log(Level.WARN, "Attempted to execute a null operation.");
			return;
		}

		// Check if the operation is an AddOperation
		if (op instanceof AddOperation) {
			// Cast the operation to AddOperation
			AddOperation addOp = (AddOperation) op;
			// Create a new Recipe object from the AddOperation
			Recipe rcpe = new Recipe(addOp.getRecipe().getTitle(), addOp.getRecipe().getRecipe(), addOp.getRecipe().getAuthor(), addOp.getRecipe().getTimestamp());
			// Add the new recipe to the recipes list
			this.recipes.add(rcpe);
			// Update the summary with the timestamp of the added recipe
			this.summary.updateTimestamp(addOp.getRecipe().getTimestamp());
			// Add the operation to the log
			this.log.add(op);
			// Update the acknowledgment matrix with the current summary
			this.ack.update(id, summary);
		} 
		// Check if the operation is a RemoveOperation
		else if (op instanceof RemoveOperation) {
			// Cast the operation to RemoveOperation
			RemoveOperation removeOp = (RemoveOperation) op;
			// Remove the recipe with the specified title
			removeRecipe(removeOp.getRecipeTitle());
			// Update the acknowledgment matrix with the current summary
			ack.update(id, summary);
		} 
		// Log a warning if the operation type is unknown
		else {
			LSimLogger.log(Level.WARN, "Unknown operation type executed: " + op.getClass().getName());
		}
	}


}
