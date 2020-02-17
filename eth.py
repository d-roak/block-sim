
from collections import defaultdict, deque
from threading import Thread
import gc
import logging
import sys
import yaml
import random
import pickle
import hashlib
import json
import math
import statistics
import merkletools

from sim import sim
import utils

PING_MSG, PONG_MSG, FINDNODE_MSG, NEIGHBORS_MSG = "0x01 PING", "0x02 PONG", "0x03 FINDNODE", "0x04 NEIGHBORS"

# New Model Syncing PV62
STATUS_MSG, NEWBLOCKHASHES_MSG, TRANSACTIONS_MSG, GETBLOCKHEADERS_MSG, BLOCKHEADERS_MSG, GETBLOCKBODIES_MSG, BLOCKBODIES_MSG, NEWBLOCK_MSG = \
"0x00 STATUS", "0x01 NEWBLOCKHASHES", "0x02 TRANSACTIONS", "0x03 GETBLOCKHEADERS", "0x04 BLOCKHEADERS", "0x05 GETBLOCKBODIES", "0x06 BLOCKBODIES", "0x07 NEWBLOCK"


CURRENT_CYCLE, CURRENT_TIME, MEMB_MSGS_RECEIVED, MEMB_MSGS_SENT, DISS_MSGS_RECEIVED, DISS_MSGS_SENT, ID, ID_SHA, DB, TABLE, BLOCKCHAIN, \
BLOCKCHAIN_HASHES, KNOWN_TXS, KNOWN_BLOCKS, ANN_TXS, SENT_STATUS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15

# used only to check if the node is already in the network
# nodes don't have an overview of the network
NETWORK_NODES = []
REAL_BLOCKCHAIN = []
TX_NUMBER = 0

#used for recursive lookup
neighborsQueue = {}

def init():
	global nodeState

	for nodeId in nodeState:
		# init k-buckets
		for i in range(0, bucketsNumber):
			nodeState[nodeId][TABLE][i] = deque([ ])

		sim.schedulleExecution(CYCLE, nodeId)

def CYCLE(self):
	global nodeState, TX_NUMBER

	if self == 0:
		logger.info('node: {} cycle: {}'.format(self, nodeState[self][CURRENT_CYCLE]))
		print("Cycle: ", nodeState[self][CURRENT_CYCLE], "/", nbCycles-1)

	if self not in nodeState:
		return

	if self not in NETWORK_NODES:
		# join the network
		if random.random() <= probJoin:
			join(self)	
	else:
		if len(getNeighbors(self)) == 0:
			join(self)

		lifeCheckDbTable(self)

		# Optimize
		#if nodeState[self][CURRENT_CYCLE]%5 == 0:
		for n in nodeState[self][DB]:
			sim.send(PING, n, self, PING_MSG)
			#nodeState[self][MEMB_MSGS_SENT] += 1

		if random.random() <= probLookup:
			try:
				p = Thread(target=lookup, args=[self, self, False])
				p.start()
				p.join()
				#lookup(self, self, False)
			except Exception as e:
				print(e.with_traceback())

		# Message dissemination
		neighbors = getNeighbors(self)

		# Blockchain maintenance
		# Create transactions
		if random.random() > probTxCreate:
			for _ in range(0, int(random.random()*maxTxCreate)):
				t = generateTx(TX_NUMBER)
				TX_NUMBER += 1
				nodeState[self][KNOWN_TXS][t.getHash()] = t

		# Announcements
		for n in random.sample(neighbors, int(len(neighbors)*floodPercentage)):
			sim.send(NEWBLOCKHASHES, n, self, NEWBLOCKHASHES_MSG, list(nodeState[self][KNOWN_BLOCKS].keys()))
			nodeState[self][DISS_MSGS_SENT] += 1

		# Proccess known blocks
		for _, b in nodeState[self][KNOWN_BLOCKS].copy().items():
			# Remove known txs that are already in blocks
			txs = list(b.getBody()[1])
			for t in txs:
				if t.getHash() in nodeState[self][KNOWN_TXS]:
					del nodeState[self][KNOWN_TXS][t.getHash()]

			# Add to blockchain
			if b.getHash() not in nodeState[self][BLOCKCHAIN_HASHES]:
				if addBlockToBlockchain(nodeState[self][BLOCKCHAIN], b):
					nodeState[self][BLOCKCHAIN_HASHES][b.getHash()] = b
				addBlockToBlockchain(REAL_BLOCKCHAIN, b)

			del nodeState[self][KNOWN_BLOCKS][b.getHash()]

		# Create block
		# Stop creating blocks at 90% of cycles
		if nodeState[self][CURRENT_CYCLE] < 0.9 * nbCycles:
			if len(nodeState[self][KNOWN_TXS]) > minTxPerBlock and self >= 0 and self <= miners:
				b = generateBlock(self, nodeState[self][KNOWN_TXS].values())
				nodeState[self][KNOWN_TXS].clear()
				nodeState[self][KNOWN_BLOCKS][b.getHash()] = b

				# Send NewBlock to math.sqrt(neighbors)
				# Send NewBlockHashes to remaining
				rootSample = random.sample(neighbors, int(math.sqrt(len(neighbors))))
				for n in neighbors:
					if n in rootSample:
						sim.send(NEWBLOCK, n, self, NEWBLOCK_MSG, b)
					else:
						sim.send(NEWBLOCKHASHES, n, self, NEWBLOCKHASHES_MSG, [b.getHash()])
					nodeState[self][DISS_MSGS_SENT] += 1
		
		for t in nodeState[self][KNOWN_TXS]:
			if not nodeState[self][ANN_TXS].get(t):
				nodeState[self][ANN_TXS][t] = []
		for n in getNeighbors(self):
			sendTxs = []
			for h in nodeState[self][ANN_TXS]:
				if n not in nodeState[self][ANN_TXS][h] and h in nodeState[self][KNOWN_TXS]:
					sendTxs.append(nodeState[self][KNOWN_TXS][h])
			if sendTxs:
				sim.send(TRANSACTIONS, n, self, TRANSACTIONS_MSG, sendTxs)
				nodeState[self][DISS_MSGS_SENT] += 1

	nodeState[self][CURRENT_CYCLE] += 1
	nodeState[self][CURRENT_TIME] += nodeCycle
	if nodeState[self][CURRENT_CYCLE] < nbCycles:
		sim.schedulleExecution(CYCLE, self)

def join(self):
	if self not in NETWORK_NODES:
		NETWORK_NODES.append(self)
	if len(NETWORK_NODES) < 2:
		return
	destNode = random.choice(NETWORK_NODES)
	while destNode == self:
		destNode = random.choice(NETWORK_NODES)
	addEntryDb(self, destNode)
	addEntryBucket(self, destNode)
	sim.send(FINDNODE, destNode, self, FINDNODE_MSG, destNode)
	nodeState[self][SENT_STATUS].append(destNode)
	sim.send(STATUS, destNode, self, STATUS_MSG, nodeState[self][BLOCKCHAIN][-1].getHash(), nodeState[self][BLOCKCHAIN][-1].getNumber())
	nodeState[self][DISS_MSGS_SENT] += 1
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
	addEntryDb(self, source)
	updateEntryPingDb(self, source, nodeState[self][CURRENT_TIME])

	sim.send(PONG, source, self, PONG_MSG)
	#nodeState[self][MEMB_MSGS_SENT] += 1


def PONG(self, source, msg):
	#logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	#nodeState[self][MEMB_MSGS_RECEIVED] += 1

	addEntryDb(self, source)
	updateEntryPongDb(self, source, nodeState[self][CURRENT_TIME])


def FINDNODE(self, source, msg, target):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][MEMB_MSGS_RECEIVED] += 1

	addEntryDb(self, source)
	addEntryBucket(self, source)

	sim.send(NEIGHBORS, source, self, NEIGHBORS_MSG, lookupNeighbors(self, target))
	nodeState[self][MEMB_MSGS_SENT] += 1


def NEIGHBORS(self, source, msg, nodes):
	logger.info("Node: {} Received: {} From: {} Neighbors: {}".format(self, msg, source, nodes))
	nodeState[self][MEMB_MSGS_RECEIVED] += 1

	addEntryDb(self, source)

	global neighborsQueue
	# for recursive lookup
	neighborsQueue[source] = nodes

	for n in nodes:
		addEntryDb(self, n)
		addEntryBucket(self, n)


# Message Dissemination Messages

def STATUS(self, source, msg, bestHash, blockNumber):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	addEntryDb(self, source)

	if source not in nodeState[self][SENT_STATUS]:
		sim.send(STATUS, source, self, STATUS_MSG, nodeState[self][BLOCKCHAIN][-1].getHash(), nodeState[self][BLOCKCHAIN][-1].getNumber())
		nodeState[self][DISS_MSGS_SENT] += 1
	else:
		nodeState[self][SENT_STATUS].remove(source)

	# Send Transactions
	sim.send(TRANSACTIONS, source, self, TRANSACTIONS_MSG, list(nodeState[self][KNOWN_TXS].values()))
	nodeState[self][DISS_MSGS_SENT] += 1

	# Request Headers
	if nodeState[self][BLOCKCHAIN][-1].getNumber() < blockNumber:
		sim.send(GETBLOCKHEADERS, source, self, GETBLOCKHEADERS_MSG, bestHash, 1)
		nodeState[self][DISS_MSGS_SENT] += 1


def NEWBLOCKHASHES(self, source, msg, hashes):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	tmp = []
	for h in hashes:
		if h not in nodeState[self][KNOWN_BLOCKS].keys():
			tmp.append(h)

	sim.send(GETBLOCKBODIES, source, self, GETBLOCKBODIES_MSG, tmp)
	nodeState[self][DISS_MSGS_SENT] += 1


def TRANSACTIONS(self, source, msg, txs):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	if verifyTxs(self, txs):
		for t in txs:
			nodeState[self][KNOWN_TXS][t.getHash()] = t
			if not nodeState[self][ANN_TXS].get(t.getHash()):
				nodeState[self][ANN_TXS][t.getHash()] = []
			if source not in nodeState[self][ANN_TXS][t.getHash()]:
				nodeState[self][ANN_TXS][t.getHash()].append(source)

		# Propagation
		for n in getNeighbors(self):
			tmp = []
			for t in txs:
				if n not in nodeState[self][ANN_TXS][t.getHash()]:
					nodeState[self][ANN_TXS][t.getHash()].append(n)
					tmp.append(t)
	
			if tmp:
				sim.send(TRANSACTIONS, n, self, TRANSACTIONS_MSG, tmp)
				nodeState[self][DISS_MSGS_SENT] += 1


def GETBLOCKHEADERS(self, source, msg, hash, reverse):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	headers = []
	b_index = nodeState[self][BLOCKCHAIN].index(nodeState[self][BLOCKCHAIN_HASHES].get(hash))
	min_range = 0

	if reverse == 0:
		min_range = len(nodeState[self][BLOCKCHAIN]) - b_index
	elif reverse == 1:
		min_range = b_index

	for i in range(0, min(maxBlockHeaders, min_range)):
		if reverse == 0:
			headers.append(nodeState[self][BLOCKCHAIN][b_index + i].getHeader())
		elif reverse == 1:
			headers.append(nodeState[self][BLOCKCHAIN][b_index - i].getHeader())

	if headers:
		sim.send(BLOCKHEADERS, source, self, BLOCKHEADERS_MSG, headers)
		nodeState[self][DISS_MSGS_SENT] += 1


def BLOCKHEADERS(self, source, msg, headers):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	hashes = []
	if verifyHeaders(self, headers):
		for b in nodeState[self][BLOCKCHAIN]:
			if b.getHeader() in headers:
				hashes.append(b.getHash())   

	if hashes:
		sim.send(GETBLOCKBODIES, source, self, GETBLOCKBODIES_MSG, hashes)
		nodeState[self][DISS_MSGS_SENT] += 1


def GETBLOCKBODIES(self, source, msg, hashes):
	# Send full block
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	blocks = []
	for h in hashes:
		if h in nodeState[self][KNOWN_BLOCKS].keys():
			blocks.append(nodeState[self][KNOWN_BLOCKS].get(h))
		elif h in nodeState[self][BLOCKCHAIN_HASHES].keys():
			blocks.append(nodeState[self][BLOCKCHAIN_HASHES].get(h))

	if blocks:
		sim.send(BLOCKBODIES, source, self, BLOCKBODIES_MSG, blocks)
		nodeState[self][DISS_MSGS_SENT] += 1


def BLOCKBODIES(self, source, msg, blocks):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	if verifyBlocks(self, blocks):
		for b in blocks:
			nodeState[self][KNOWN_BLOCKS][b.getHash()] = b


def NEWBLOCK(self, source, msg, block):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	nodeState[self][KNOWN_BLOCKS][block.getHash()] = block


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
	# TABLE					# private
	# BLOCKCHAIN			# private
	# BLOCKCHAIN_HASHES		# private
	# KNOWN_TXS				# private
	# KNOWN_BLOCKS			# private
	# ANN_TXS				# private
	# SENT_STATUS			# private

	return [0, 0, 0, 0, 0, id, hashlib.sha256(str(id).encode('utf-8')).hexdigest(), dict(), dict(), [], dict(), dict(), dict(), dict(), []]


# DB (not persistent)
# saves every node that the client has seen 

def addEntryDb(self, node):
	global nodeState
	# [node] (lastPing, lastPong, failedResponses)
	# failedResponses stands for failed responses to FINDNODE message
	if node in nodeState[self][DB] or self == node:
		return

	currentTime = nodeState[self][CURRENT_TIME]
	nodeState[self][DB][node] = (currentTime, currentTime, 0)

def removeEntryDb(self, node):
	logger.info("Node: {} Removed (DB) Neighbor: {}".format(self, node))

	del nodeState[self][DB][node]

def updateEntryPingDb(self, node, lastPing):
	tuple = nodeState[self][DB][node]
	nodeState[self][DB][node] = (lastPing, tuple[1], tuple[2])

def updateEntryPongDb(self, node, lastPong):
	tuple = nodeState[self][DB][node]
	nodeState[self][DB][node] = (tuple[0], lastPong, tuple[2])

def incrementFailedDb(self, node):
	tuple = nodeState[self][DB][node]
	nodeState[self][DB][node] = (tuple[0], tuple[1], tuple[2] + 1)

def resetFailedDb(self, node):
	tuple = nodeState[self][DB][node]
	nodeState[self][DB][node] = (tuple[0], tuple[1], 0)


# Table /Buckets

def addEntryBucket(self, node):
	global nodeState
	# [kadDistance, max 256] nodeId, max 16
	if self == node:
		return

	pos = kadDistance(self, node)

	if node in nodeState[self][TABLE][pos]:
		updateBucket(self, node)
	elif len(nodeState[self][TABLE][pos]) < bucketSize:
		nodeState[self][TABLE][pos].appendleft(node)
	else:
		replaceEntryBucket(self, node)

def removeEntryBucket(self, node):
	logger.info("Node: {} Removed (Table) Neighbor: {}".format(self, node))

	pos = kadDistance(self, node)
	nodeState[self][TABLE][pos].remove(node)

def replaceEntryBucket(self, node):
	pos = kadDistance(self,node)

	# ignored double checking liveness
	# it will cause a cycle (creates a DoS vector)
	nodeState[self][TABLE][pos].pop()
	nodeState[self][TABLE][pos].appendleft(node)

def updateBucket(self, node):
	# update the order of nodes (liveness)
	pos = kadDistance(self, node)

	if node in nodeState[self][TABLE][pos]:
		nodeState[self][TABLE][pos].remove(node)
		nodeState[self][TABLE][pos].appendleft(node)
	else:
		return

def getNeighbors(self):
	tmp = []
	for k in nodeState[self][TABLE]:
		for n in nodeState[self][TABLE][k]:
			tmp.append(n)
		
	return tmp

def lifeCheckDbTable(self):
	# if the last pong received is older than 1 day, that node will be removed from the db
	# (it will never happen in the simulation)
	# if node fails to respond to findnode more than 4 times in a row, it will be removed from the table

	for k in nodeState[self][DB]:
		currentTime = nodeState[self][CURRENT_TIME]
		if currentTime - nodeState[self][DB][k][1] > 24 * 60 * 1000:
			removeEntryDb(self, k)
			continue

		if nodeState[self][DB][k][2] > 3:
			removeEntryBucket(self, k)

# Helpers

def lookupNeighbors(self, target):
	temp = {}
	for k in nodeState[self][TABLE].keys():
		for n in nodeState[self][TABLE][k]:
			pos = kadDistance(n, target)
			if pos not in temp.keys():
				temp[pos] = deque([ ])
			temp[pos].appendleft(n)

	if not temp:
		return []
	return temp[min(temp.keys())]

def lookup(self, target, stopMatch):
	# recursive lookup
	global neighborsQueue
	global nodeState

	asked = {}
	seen = []
	result = []
	pendingQueries = 0

	result = lookupNeighbors(self, target)
	while True:
		for n in result:
			if pendingQueries >= alpha:
				break
			if n not in asked.keys():
				asked[n] = nodeState[self][CURRENT_TIME]
				pendingQueries += 1
				addEntryDb(self, n)
				addEntryBucket(self, n)
				sim.send(FINDNODE, n, self, FINDNODE_MSG, target)
				nodeState[self][MEMB_MSGS_SENT] += 1

		if pendingQueries == 0:
			break

		for queried in asked.keys():
			if asked[queried] == 0 or queried not in neighborsQueue:
				continue

			response = neighborsQueue[queried]
			for n in response:
				if n not in seen:
					seen.append(n)
					result.append(n)
					if stopMatch and n == target:
						return result
			asked[queried] = 0
			pendingQueries -= 1

		for n in asked.keys():
			if asked[n] == 0:
				continue

			t = (nodeState[self][CURRENT_TIME] - asked[n]) * 1000
			if t > lookupTimeout:
				pendingQueries = 0

	return result

def kadDistance(n1, n2):
	#dist = int.from_bytes(nodeState[n1][ID_SHA], sys.byteorder) ^ int.from_bytes(nodeState[n2][ID_SHA], sys.byteorder)
	dist = int(nodeState[n1][ID_SHA], 16) ^ int(nodeState[n2][ID_SHA], 16)
	if dist == 0:
		return 0
	return int(math.floor(math.log(dist, 2)))


# Blockchain functions

def generateGenesisBlock():
	header = ("0", "0", 0)
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


# Simulator and logging functions

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
		if n not in conns_bound:
			conns_bound[n] = [0, len(getNeighbors(n))]
		#blockchain_local = []
		db = []
		table = {}
		lat_sum = 0

		if len(REAL_BLOCKCHAIN) == len(nodeState[n][BLOCKCHAIN]): 
			bc_right = True
		else:
			bc_right = False
			bc_wrong += 1
		for i, b in enumerate(nodeState[n][BLOCKCHAIN]):
			#blockchain_local.append({
			#	"prev_hash": b.getHeader()[0],
			#	"hash": b.getHash(),
			#})
			if bc_right and REAL_BLOCKCHAIN[i] is None or REAL_BLOCKCHAIN[i].getHash() != b.getHash():
				bc_right = False
				bc_wrong += 1

		for i in nodeState[n][DB]:
			db.append(i)
		for row in nodeState[n][TABLE]:
			if nodeState[n][TABLE][row]:
				table[row] = []
				for i in list(nodeState[n][TABLE][row]):			
					table[row].append({
						"id": i,
						"latency": sim.getMessageLatency(n, i)
					})
		
		for k in getNeighbors(n):
			lat_sum += sim.getMessageLatency(n, k)
			lat_in_sum += sim.getMessageLatency(k, n)
			lat_out_sum += sim.getMessageLatency(n, k)
			if k not in conns_bound:
				conns_bound[k] = [1, len(getNeighbors(k))]
			else:
				conns_bound[k][0] += 1 
		
		avg_latency = 0
		if len(getNeighbors(n)) != 0:
			avg_latency = lat_sum/len(getNeighbors(n))

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
			#"db": db,
			#"table": table
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
	global nbNodes, nbCycles, probJoin, probLookup, nodeState, nodeCycle, bucketSize, bucketsNumber,\
	alpha, lookupTimeout, maxBlockHeaders, miners, minTxPerBlock, probTxCreate, maxTxCreate, floodPercentage

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
	bucketSize = config['bucketSize']
	bucketsNumber = config['bucketsNumber']
	alpha = config['alpha']
	lookupTimeout = config['lookupTimeout']
	maxBlockHeaders = config['maxBlockHeaders']
	miners = config['miners']
	floodPercentage = config['floodPercentage']
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

	if len(sys.argv) < 2:
		logger.error("Invocation: ./eth_memb.py <conf_out_dir>")
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
