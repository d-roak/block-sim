
from collections import defaultdict, deque
from collections import OrderedDict
from multiprocessing import Process
import gc
import logging
import sys
import yaml
import random
import pickle
import json
import math
import merkletools
import statistics
import hashlib

from sim import sim
import utils

PING_MSG, PONG_MSG, FINDNODE_MSG, NEIGHBORS_MSG, CONNECT_MSG, ACKCONNECT_MSG, REJECTCONNECT_MSG, FORCECONNECT_MSG, DISCONNECT_MSG = \
"PING", "PONG", "FINDNODE", "NEIGHBORS", "CONNECT", "ACKCONNECT", "REJECTCONNECT", "FORCECONNECT", "DISCONNECT"

STATUS_MSG, BLOCKHASHES_MSG, GETBLOCKS_MSG, BLOCK_MSG, TXS_MSG, PRUNE_MSG = \
"STATUS", "BLOCKHASHES", "GETBLOCKS", "BLOCK", "TRANSACTIONS", "PRUNE"

CURRENT_CYCLE, CURRENT_TIME, MEMB_MSGS_RECEIVED, MEMB_MSGS_SENT, DISS_MSGS_RECEIVED, DISS_MSGS_SENT, ID, ID_SHA, DB, NEIGHBS, \
BTREE, BLOCKCHAIN, BLOCKCHAIN_HASHES, KNOWN_TXS, KNOWN_BLOCKS, QUEUED_TXS, QUEUED_BLOCKS, SENT_STATUS = \
0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17

EAGER, LAZY = 0, 1

# used only to check if the node is already in the network (simulator)
# nodes don't have an overview of the network
NETWORK_NODES = []
REAL_BLOCKCHAIN = []
TX_NUMBER = 0

def init():
	global nodeState

	for nodeId in nodeState:
		sim.schedulleExecution(CYCLE, nodeId)

def CYCLE(self):
	global nodeState, TX_NUMBER

	if self not in nodeState:
		return

	if self == 0:
		logger.info('node: {} cycle: {}'.format(self, nodeState[self][CURRENT_CYCLE]))
		print("Cycle: ", nodeState[self][CURRENT_CYCLE], "/", nbCycles-1)

	if self not in NETWORK_NODES:
		# join the network
		if random.random() <= probJoin:
			join(self)
	else:
		'''
		if neighbsSize(self) < tableSize and len(nodeState[self][DB]) > 2:
			tmp = random.choices(nodeState[self][DB], k=2)
			for n in tmp:
				addEntryNeighbs(self, n)
				sim.send(FINDNODE, n, self, FINDNODE_MSG)
		'''
		# lifecheck and cycle ping
		lifeCheckDBNeighbs(self)
		for n in nodeState[self][DB]:
			sim.send(PING, n, self, PING_MSG)
			#nodeState[self][MEMB_MSGS_SENT] += 1
		
		# lookup for new neighbors
		if neighbsSize(self) < neighbThreshold:
			lookup(self)
		
		# TODO reduce messages while maintaining low latency
		#if random.random() <= probLookup:
		#	lookup(self)

		# Message dissemination		
		# Blockchain maintenance
		# Proccess queued blocks
		for b in nodeState[self][QUEUED_BLOCKS]:
			# Remove known txs that are already in blocks
			txs = list(b.getBody()[1])
			for t in txs:
				if t in nodeState[self][QUEUED_TXS]:
					nodeState[self][QUEUED_TXS].remove(t)

			# Add to blockchain
			if b.getHash() not in nodeState[self][BLOCKCHAIN_HASHES]:
				if addBlockToBlockchain(nodeState[self][BLOCKCHAIN], b):
					nodeState[self][BLOCKCHAIN_HASHES][b.getHash()] = b
				addBlockToBlockchain(REAL_BLOCKCHAIN, b)
		nodeState[self][QUEUED_BLOCKS].clear()

		# Create block & Txs
		# Stop creating blocks and txs at 90% of cycles
		if nodeState[self][CURRENT_CYCLE] < 0.9 * nbCycles:
			if random.random() > probTxCreate:
				newTxs = []
				for _ in range(0, int(random.random()*maxTxCreate)):
					t = generateTx(TX_NUMBER)
					TX_NUMBER += 1
					nodeState[self][KNOWN_TXS][t.getHash()] = t
					nodeState[self][QUEUED_TXS].append(t)
					newTxs.append(t)
				for n in nodeState[self][NEIGHBS]:
					sim.send(TXS, n, self, TXS_MSG, newTxs)
					nodeState[self][DISS_MSGS_SENT] += 1
			if len(nodeState[self][QUEUED_TXS]) > minTxPerBlock and self >= 0 and self <= miners:
				b = generateBlock(self, nodeState[self][QUEUED_TXS])
				nodeState[self][QUEUED_TXS].clear()
				nodeState[self][KNOWN_BLOCKS][b.getHash()] = b
				nodeState[self][QUEUED_BLOCKS].append(b)
				for n in nodeState[self][NEIGHBS]:
					sim.send(BLOCK, n, self, BLOCK_MSG, b)
					nodeState[self][DISS_MSGS_SENT] += 1
		
	nodeState[self][CURRENT_CYCLE] += 1
	nodeState[self][CURRENT_TIME] += nodeCycle
	if nodeState[self][CURRENT_CYCLE] < nbCycles:
		sim.schedulleExecution(CYCLE, self)

def join(self):
	NETWORK_NODES.append(self)
	if len(NETWORK_NODES) < 2:
		return
	destNode = random.choice(NETWORK_NODES)
	while destNode == self:
		destNode = random.choice(NETWORK_NODES)
	addEntryDB(self, destNode)
	addEntryNeighbs(self, destNode)
	sim.send(FINDNODE, destNode, self, FINDNODE_MSG)
	nodeState[self][SENT_STATUS].append(destNode)
	sim.send(STATUS, destNode, self, STATUS_MSG, nodeState[self][BLOCKCHAIN][-1].getHash(), nodeState[self][BLOCKCHAIN][-1].getNumber())
	nodeState[self][DISS_MSGS_SENT] += 1
	nodeState[self][MEMB_MSGS_SENT] += 1

def lookup(self):
	for n in nodeState[self][NEIGHBS]:
		sim.send(FINDNODE, n, self, FINDNODE_MSG)
		nodeState[self][MEMB_MSGS_SENT] += 1

'''
Model

def RVC_MSG(self, source, msg):
	# logging
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][MSGS_RECEIVED] += 1

	# processing
	....

	# response (optional)
	sim.send(RESP, source, self, RESP_MSG)
	nodeState[self][MSGS_SENT] += 1
'''

def PING(self, source, msg):
	#logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	#nodeState[self][MEMB_MSGS_RECEIVED] += 1

	addEntryDB(self, source)

	sim.send(PONG, source, self, PONG_MSG)
	#nodeState[self][MSGS_SENT] += 1

def PONG(self, source, msg):
	#logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	#nodeState[self][MSGS_RECEIVED] += 1

	updateEntryPongDB(self, source, nodeState[self][CURRENT_TIME])

def FINDNODE(self, source, msg):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][MEMB_MSGS_RECEIVED] += 1

	addEntryDB(self, source)
	addEntryNeighbs(self, source)

	sim.send(NEIGHBORS, source, self, NEIGHBORS_MSG, lookupNeighbors(self))
	nodeState[self][MEMB_MSGS_SENT] += 1

def NEIGHBORS(self, source, msg, nodes):
	logger.info("Node: {} Received: {} From: {} Neighbors: {}".format(self, msg, source, nodes))
	nodeState[self][MEMB_MSGS_RECEIVED] += 1

	addEntryDB(self, source)
	addEntryNeighbs(self, source)

	for n in nodes:
		if self == n:
			continue
		addEntryDB(self, source)
		addEntryNeighbs(self, n)


# Dissemination Messages
def STATUS(self, source, msg, bestHash, blockNum):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	if source not in nodeState[self][SENT_STATUS]:
		sim.send(STATUS, source, self, STATUS_MSG, nodeState[self][BLOCKCHAIN][-1].getHash(), nodeState[self][BLOCKCHAIN][-1].getNumber())
		nodeState[self][DISS_MSGS_SENT] += 1
	else:
		nodeState[self][SENT_STATUS].remove(source)

	# Send Transactions
	sim.send(TXS, source, self, TXS_MSG, nodeState[self][QUEUED_TXS])
	nodeState[self][DISS_MSGS_SENT] += 1

	# Request Headers
	if nodeState[self][BLOCKCHAIN][-1].getNumber() < blockNum:
		sim.send(GETBLOCKS, source, self, GETBLOCKS_MSG, [bestHash], True)
		nodeState[self][DISS_MSGS_SENT] += 1

def BLOCKHASHES(self, source, msg, hashes):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	tmp = []
	for h in hashes:
		if h not in nodeState[self][KNOWN_BLOCKS]:
			tmp.append(h)
	
	if tmp:
		sim.send(GETBLOCKS, source, self, GETBLOCKS_MSG, tmp, False)
		nodeState[self][DISS_MSGS_SENT] += 1


def GETBLOCKS(self, source, msg, hashes, sync):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	if sync:
		retrieve = []
		b_index = nodeState[self][BLOCKCHAIN].index(nodeState[self][BLOCKCHAIN_HASHES].get(hashes[0]))
		for i in range(0, b_index):
			retrieve.append(nodeState[self][BLOCKCHAIN][b_index - i].getHeader())
	else:
		retrieve = list(hashes)

	for h in retrieve:
		if h in nodeState[self][KNOWN_BLOCKS]:
			sim.send(BLOCK, source, self, BLOCK_MSG, nodeState[self][KNOWN_BLOCKS][h])
			nodeState[self][DISS_MSGS_SENT] += 1


def BLOCK(self, source, msg, block):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	prune = False
	if block.getHash() not in nodeState[self][KNOWN_BLOCKS]:
		nodeState[self][KNOWN_BLOCKS][block.getHash()] = block
		nodeState[self][QUEUED_BLOCKS].append(block)
		eagerList = list(nodeState[self][BTREE][EAGER])
		lazyList = list(nodeState[self][BTREE][LAZY])
		if source in eagerList:
			eagerList.remove(source)
		if source in lazyList:
			lazyList.remove(source)
		for n in eagerList:
			sim.send(BLOCK, n, self, BLOCK_MSG, block)
			nodeState[self][DISS_MSGS_SENT] += 1
		for n in lazyList:
			sim.send(BLOCKHASHES, n, self, BLOCKHASHES_MSG, [block.getHash()])
			nodeState[self][DISS_MSGS_SENT] += 1
		graftBTree(self, source)
	else:
		prune = True

	if prune:	
		pruneBTree(self, source)
		sim.send(PRUNE, source, self, PRUNE_MSG)
		nodeState[self][DISS_MSGS_SENT] += 1


def TXS(self, source, msg, txs):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	disseminate = False
	prune = False
	for t in txs:
		if t.getHash() not in nodeState[self][KNOWN_TXS]:
			disseminate = True
			nodeState[self][KNOWN_TXS][t.getHash()] = t
			nodeState[self][QUEUED_TXS].append(t)
			graftBTree(self, source)
		else:
			prune = True

	if prune:
		pruneBTree(self, source)
		sim.send(PRUNE, source, self, PRUNE_MSG)
		nodeState[self][DISS_MSGS_SENT] += 1

	if disseminate:
		dissList = list(nodeState[self][BTREE][EAGER])
		if source in dissList:
			dissList.remove(source)
		for n in dissList:
			sim.send(TXS, n, self, TXS_MSG, txs)
			nodeState[self][DISS_MSGS_SENT] += 1


def PRUNE(self, source, msg):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	pruneBTree(self, source)


# Node functions
def createNode(id):
	# CURRENT_CYCLE			# simulator
	# MEMB_MSGS_RECEIVED	# stats
	# MEMB_MSGS_SENT		# stats
	# DISS_MSGS_RECEIVED	# stats
	# DISS_MSGS_SENT		# stats
	# ID					# public
	# ID_SHA				# public
	# DB					# private
	# NEIGHBS				# private
	# BTREE					# private (eager, lazy)
	# BLOCKCHAIN			# private
	# BLOCKCHAIN_HASHES		# private
	# KNOWN_TXS				# private
	# KNOWN_BLOCKS			# private
	# QUEUED_TXS			# private
	# QUEUED_BLOCKS			# private
	# SENT_STATUS			# private

	return [0, 0, 0, 0, 0, id, hashlib.sha256(str(id).encode('utf-8')).hexdigest(), dict(), dict(), ([], []), [], dict(), dict(), dict(), [], [], []]


# DB (not persistent)
# saves every node that the client has seen 

def addEntryDB(self, node):
	# [node] (lastPong)
	if node in nodeState[self][DB] or self == node:
		return

	nodeState[self][DB][node] = nodeState[self][CURRENT_TIME]

def removeEntryDB(self, node):
	del nodeState[self][DB][node]

def updateEntryPongDB(self, node, lastPong):
	nodeState[self][DB][node] = lastPong

# Neighbs
def neighbsSize(self):
	return len(nodeState[self][NEIGHBS])

def addEntryNeighbs(self, node):
	# [node] (failedResponses)
	if hasEntryNeighbs(self, node) or self == node:
		return
	
	if neighbsSize(self) < tableSize:
		nodeState[self][NEIGHBS][node] = 0
		updateBTree(self, add = True)
		return

	worst = node
	for n in nodeState[self][NEIGHBS]:
		if proximityID(self, worst) < proximityID(self, n):
			worst = n
		if probeLatency(self, worst) < probeLatency(self, n):
			worst = n

	if worst != node:
		replaceEntryNeighbs(self, node, worst)

def hasEntryNeighbs(self, node):
	if node in nodeState[self][NEIGHBS]:
		return True
	return False

def lookupEntryNeighbs(self, node):
	if node in nodeState[self][NEIGHBS]:
		return True
	return False

def removeEntryNeighbs(self, node):
	del nodeState[self][NEIGHBS][node]

def replaceEntryNeighbs(self, node, rmnode):
	addEntryDB(self, rmnode)
	removeEntryNeighbs(self, rmnode)
	nodeState[self][NEIGHBS][node] = 0
	updateBTree(self, add = True, remove = True)

def lifeCheckDBNeighbs(self):
	# if the last pong received is older than 1 day (24h = 86400s), that node will be removed from the db
	# (it will never happen in the simulation)
	# if node fails to respond more than 4 times in a row, it will be removed from the table
	for n in nodeState[self][DB]:
		currentTime = nodeState[self][CURRENT_TIME]
		if currentTime - nodeState[self][DB][n] > 86400:
			removeEntryDB(self, n)

		# give one as margin because pendent response
		if lookupEntryNeighbs(self, n) and nodeState[self][NEIGHBS][n] > 4:
			removeEntryNeighbs(self, n)
			updateBTree(self, remove = True)

# Helpers

def updateBTree(self, add = False, remove = False):
	if add:
		for n in nodeState[self][NEIGHBS]:
			if n not in nodeState[self][BTREE][LAZY] and n not in nodeState[self][BTREE][EAGER]:
				nodeState[self][BTREE][LAZY].append(n)
	if remove:
		for n in nodeState[self][BTREE][LAZY]:
			if n not in nodeState[self][NEIGHBS]:
				nodeState[self][BTREE][LAZY].remove(n)
		for n in nodeState[self][BTREE][EAGER]:
			if n not in nodeState[self][NEIGHBS]:
				nodeState[self][BTREE][EAGER].remove(n)

	while len(nodeState[self][BTREE][EAGER]) < tableSize*fanout:
		if len(nodeState[self][NEIGHBS]) < tableSize*fanout:
			break
		choice = random.choice(nodeState[self][BTREE][LAZY])
		graftBTree(self, choice)


def graftBTree(self, grafted):
	if grafted in nodeState[self][NEIGHBS]:
		if grafted in nodeState[self][BTREE][LAZY]: 
			nodeState[self][BTREE][LAZY].remove(grafted)
		if grafted not in nodeState[self][BTREE][EAGER]:
			nodeState[self][BTREE][EAGER].append(grafted)

def pruneBTree(self, pruned):
	if pruned in nodeState[self][NEIGHBS]:
		if pruned in nodeState[self][BTREE][EAGER]:
			nodeState[self][BTREE][EAGER].remove(pruned)
		if pruned not in nodeState[self][BTREE][LAZY]:
			nodeState[self][BTREE][LAZY].append(pruned)
	updateBTree(self)

def lookupNeighbors(self):
	# give a random sample of the neighbors
	tmp = []
	neighbs = list(nodeState[self][NEIGHBS].keys())
	while len(tmp) < sampleSize:
		if len(neighbs) == 0:
			return tmp
		n = random.choice(neighbs)
		tmp.append(n)
		neighbs.remove(n)

	return tmp

def proximityID(n1, n2):
	# proximity between id's | id1 - id2 |
	dist = int(nodeState[n1][ID_SHA], 16) ^ int(nodeState[n2][ID_SHA], 16)
	return int(math.floor(math.log(dist, 2)))

def probeLatency(self, dest):
	return sim.getMessageLatency(self, dest)


# Blockchain functions

def generateGenesisBlock():
	header = ("0", "0", 1231006505)
	body = ("0", [])
	block = Block(0, header, body)
	return block

def generateBlock(self, txs):
	# Simplified version without extra stuff
	# Header (prev_hash, merkle_root, timestamp)
	# Body (merkle_proof(0), txs)

	mt = merkletools.MerkleTools(hash_type="sha256")
	for t in txs:
		mt.add_leaf(t.getHash(), True)
	mt.make_tree()

	number = nodeState[self][BLOCKCHAIN][-1].getNumber() + 1
	header = (nodeState[self][BLOCKCHAIN][-1].getHash(), mt.get_merkle_root(), nodeState[self][CURRENT_TIME])
	body = (mt.get_proof(0), txs)
	block = Block(number, header, body)
	return block

def generateTx(n):
	return Tx(n)

def addBlockToBlockchain(blockchain, block):
	# Used for local blockchains and real blockchain
	for b in blockchain:
		if b.getHash() == block.getHash():
			return False

	for i, b in enumerate(blockchain):
		# Confirm prev_hash
		if b.getHash() == block.getHeader()[0]:
			# Confirm timestamp
			if block.getHeader()[2] < b.getHeader()[2]:
				return False
			elif block.getNumber() < b.getNumber():
				return False
			# Check if there are more blocks in chain
			elif blockchain[-1].getHash() != b.getHash():
				if blockchain[i + 1].getHeader()[2] > block.getHeader()[2]:
					del blockchain[i+1:]
					blockchain.append(block)
					return True
				else:
					return False
			else:		
				blockchain.append(block)
				return True
		else:
			continue
	
	return False

def verifyHeaders(self, headers):
	# TODO ???
	return True

def verifyBlocks(self, blocks):
	# TODO ???
	return True

def verifyTxs(self, txs):
	# TODO ???
	return True

# Block definition
class Block:
	def __init__(self, number, header, body):
		# Simplified version without extra stuff
		# Header (prev_hash, merkle_root, timestamp)
		# Body (merkle_proof(0), txs)
		self.number = number
		self.nonce = random.random() * 10000000
		self.header = header
		self.body = body

	def __str__(self):
		return self.getHash()

	def __eq__(self, other):
		if self.nonce == other.nonce:
			return 0
		elif self.nonce < other.nonce:
			return -1
		else:
			return 1

	def getNumber(self):
		return self.number

	def getHeader(self):
		return self.header

	def getHash(self):
		#sha256(prev_hash, merkle_root, timestamp)
		h = hashlib.sha256()
		h.update(self.header[0].encode('utf-8'))
		h.update(self.header[1].encode('utf-8'))
		h.update(str(self.header[2]).encode('utf-8'))
		return h.hexdigest()

	def getBody(self):
		return self.body


# Transaction definition
class Tx:
	def __init__(self, n):
		self.nonce = random.random() * 10000000
		self.n = n

	def __str__(self):
		return self.getHash()

	def __eq__(self, other):
		if self.nonce == other.nonce:
			return 0
		elif self.nonce < other.nonce:
			return -1
		else:
			return 1
	
	def getHash(self):
		return hashlib.sha256(str(self.n).encode('utf-8')).hexdigest()


def wrapup(dir):
	logger.info("Wrapping up")
	data = {}
	# conns_bound = [in, out]
	conns_bound = {}
	lat_in_sum = 0
	lat_out_sum = 0
	bc_wrong = 0
	
	# Statistics
	membMsgsReceived = list(map(lambda x: nodeState[x][MEMB_MSGS_RECEIVED], nodeState))
	membMsgsSent = list(map(lambda x: nodeState[x][MEMB_MSGS_SENT], nodeState))
	dissMsgsReceived = list(map(lambda x: nodeState[x][DISS_MSGS_RECEIVED], nodeState))
	dissMsgsSent = list(map(lambda x: nodeState[x][DISS_MSGS_SENT], nodeState))
	totalMsgsReceived = list(membMsgsReceived) + list(dissMsgsReceived)
	totalMsgsSent = list(membMsgsSent) + list(dissMsgsSent)
	data['stats'] = {}
	data['stats'].update({
		"numCycles": nbCycles,
		"numNodes": nbNodes,
		"receivedMsgs_min": min(totalMsgsReceived),
		"receivedMsgs_max": max(totalMsgsReceived),
		"receivedMsgs_avg": sum(totalMsgsReceived)/len(totalMsgsReceived),
		"receivedMsgs_memb": sum(membMsgsReceived),
		"receivedMsgs_diss": sum(dissMsgsReceived),
		"receivedMsgs_total": sum(totalMsgsReceived),
		"sentMsgs_min": min(totalMsgsSent),
		"sentMsgs_max": max(totalMsgsSent),
		"sentMsgs_avg": sum(totalMsgsSent)/len(totalMsgsSent),
		"sentMsgs_memb": sum(membMsgsSent),
		"sentMsgs_diss": sum(dissMsgsSent),
		"sentMsgs_total": sum(totalMsgsSent),
	})

	# Nodes
	data['nodes'] = []
	for n in nodeState:
		#db = []
		#neighbs = {}
		blockchain_local = []
		lat_sum = 0
		
		if n not in conns_bound:
			conns_bound[n] = [1, neighbsSize(n)]

		if len(REAL_BLOCKCHAIN) == len(nodeState[n][BLOCKCHAIN]): 
			bc_right = True
		else:
			bc_right = False
			bc_wrong += 1
		for i, b in enumerate(nodeState[n][BLOCKCHAIN]):
			blockchain_local.append({
				"prev_hash": b.getHeader()[0],
				"hash": b.getHash(),
			})
			if bc_right and REAL_BLOCKCHAIN[i] and REAL_BLOCKCHAIN[i].getHash() != b.getHash():
				bc_right = False
				bc_wrong += 1

		#for i in nodeState[n][DB]:
		#	db.append(i)
		for i in nodeState[n][NEIGHBS]:
			#neighbs[i] = str(sim.getMessageLatency(n, i)) + "ms"
			lat_sum += sim.getMessageLatency(n, i)
			lat_out_sum += sim.getMessageLatency(n, i)
			lat_in_sum += sim.getMessageLatency(i, n)

			if i not in conns_bound:
				conns_bound[i] = [1, neighbsSize(i)]
			else:
				conns_bound[i][0] += 1

		avg_latency = 0
		if neighbsSize(n) != 0:
			avg_latency = lat_sum/neighbsSize(n)

		data['nodes'].append({
			"id": nodeState[n][ID],
			#"id_sha": nodeState[n][ID_SHA],
			"memb_msgs_received": nodeState[n][MEMB_MSGS_RECEIVED],
			"diss_msgs_received": nodeState[n][DISS_MSGS_RECEIVED],
			"memb_msgs_sent": nodeState[n][MEMB_MSGS_SENT],
			"diss_msgs_sent": nodeState[n][DISS_MSGS_SENT],
			"peer_avg_latency": "%0.1f ms" % (avg_latency),
			"blockchain_right": bc_right,
			#"blockchain_local": blockchain_local,
			"known_blocks": list(nodeState[n][KNOWN_BLOCKS].keys()),
			#"known_txs": list(nodeState[n][KNOWN_TXS].keys()),
			#"db": db,
			#"neighbs": neighbs
		})

	for n in conns_bound:
		data['nodes'][n].update({
			"inbound_count": conns_bound[n][0],
			"outbound_count": conns_bound[n][1]
		})

	#blockchain = []
	#for b in REAL_BLOCKCHAIN:
	#	blockchain.append({
	#		"number": b.getNumber(),
	#		"prev_hash": b.getHeader()[0],
	#		"hash": b.getHash(),
	#	})

	data['stats'].update({
		"inbound_avg_conns": sum(map(lambda x : conns_bound[x][0], conns_bound))/len(list(map(lambda x: nodeState[x], nodeState))),
		"inbound_min_conns": min(map(lambda x : conns_bound[x][0], conns_bound)),
		"inbound_max_conns": max(map(lambda x : conns_bound[x][0], conns_bound)),
		"inbound_med_conns": statistics.median(map(lambda x : conns_bound[x][0], conns_bound)),
		"outbound_avg_conns": sum(map(lambda x : conns_bound[x][1], conns_bound))/len(list(map(lambda x: nodeState[x], nodeState))),
		"outbound_min_conns": min(map(lambda x : conns_bound[x][1], conns_bound)),
		"outbound_max_conns": max(map(lambda x : conns_bound[x][1], conns_bound)),
		"outbound_med_conns": statistics.median(map(lambda x : conns_bound[x][1], conns_bound)),
		"inbound_avg_latency": "%0.1f ms" % (lat_in_sum/sum(map(lambda x : conns_bound[x][0], conns_bound))),
		"outbound_avg_latency": "%0.1f ms" % (lat_out_sum/sum(map(lambda x : conns_bound[x][1], conns_bound))),
		"blockchain_wrong": bc_wrong,
		"total_txs": TX_NUMBER,
		"total_blocks": len(REAL_BLOCKCHAIN),
		#"blockchain": blockchain,
	})

	with open(dir, 'w+') as outfile: json.dump(data, outfile, indent=2)

def configure(config):
	global nbNodes, nbCycles, probJoin, probLookup, nodeState, nodeCycle, fanout, tableSize,\
	sampleSize, neighbThreshold, miners, minTxPerBlock, probTxCreate, maxTxCreate

	IS_CHURN = config.get('CHURN', False)
	MESSAGE_LOSS = 0.0
	if IS_CHURN:
		MESSAGE_LOSS = float(config.get('MESSASE_LOSS', 0))
	if MESSAGE_LOSS > 0:
		sim.setMessageLoss(MESSAGE_LOSS)

	nbNodes = config['nbNodes']
	nbCycles = config['nbCycles']

	randomSeed = config['randomSeed']
	if randomSeed != -1:
		random.seed(randomSeed)
	probJoin = config['probJoin']
	probLookup = config['probLookup']
	fanout = config['fanout']
	tableSize = config['tableSize']
	sampleSize = config['sampleSize']
	neighbThreshold = config['neighbThreshold']
	miners = config['miners']
	minTxPerBlock = config['minTxPerBlock']
	probTxCreate = config['probTxCreate']
	maxTxCreate = config['maxTxCreate']

	latencyTablePath = config['LATENCY_TABLE']
	latencyValue = None
	try:
		with open(latencyTablePath, 'rb') as f:
			latencyTable = pickle.load(f)
	except:
		latencyTable = None
		latencyValue = int(latencyTablePath)
		logger.warn('Using constant latency value: {}'.format(latencyValue) ) 
		latencyTable = utils.checkLatencyNodes(latencyTable, nbNodes, latencyValue)

	nodeCycle = int(config['NODE_CYCLE'])
	nodeDrift = int(nodeCycle * float(config['NODE_DRIFT']))

	nodeState = defaultdict()
	genesisBlock = generateGenesisBlock()
	REAL_BLOCKCHAIN.append(genesisBlock)

	for n in range(nbNodes):
		nodeState[n] = createNode(n)
		nodeState[n][BLOCKCHAIN].append(genesisBlock)
		nodeState[n][BLOCKCHAIN_HASHES][genesisBlock.getHash()] = genesisBlock

	sim.init(nodeCycle, nodeDrift, latencyTable, 0)


if __name__ == '__main__':

	if len(sys.argv) < 3:
		logger.error("Invocation: ./cosmos_memb.py <conf_out_dir>")
		sys.exit()

	logger = logging.getLogger(__file__)
	logger.setLevel(logging.DEBUG)
	fl = logging.FileHandler(sys.argv[1] + '/log/logger.log')
	fl.setLevel(logging.INFO)
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	fl.setFormatter(formatter)

	#logger.addHandler(fl)

	confFile = sys.argv[1] + '/conf.yaml'
	f = open(confFile)

	configure(yaml.load(f))
	logger.info('Configuration done')

	init()
	logger.info('Init done')

	sim.run()
	logger.info('Run done')

	wrapup(sys.argv[1]+'/log/'+str(nbNodes)+'-'+str(nbCycles)+'-summary-'+sys.argv[2]+'.json')
	logger.info("That's all folks!")