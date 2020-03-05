
from collections import defaultdict, deque
from multiprocessing import Process
import gc
import logging
import sys
import yaml
import random
import pickle
import json
import math
import hashlib
import merkletools
import statistics
import time

from sim import sim
import utils

PING_MSG, PONG_MSG, VERSION_MSG, VERACK_MSG, GETADDR_MSG, ADDR_MSG = "PING", "PONG", "VERSION", "VERACK", "GETADDR", "ADDR"
INV_MSG, GETHEADERS_MSG, HEADERS_MSG, GETBLOCKS_MSG, GETDATA_MSG, BLOCK_MSG, TX_MSG = \
"INV", "GETHEADERS", "HEADERS", "GETBLOCKS", "GETDATA", "BLOCK", "TX"

CURRENT_CYCLE, CURRENT_TIME, MEMB_MSGS_RECEIVED, MEMB_MSGS_SENT, DISS_MSGS_RECEIVED, DISS_MSGS_SENT, ID, CONNS, \
BLOCKCHAIN, BLOCKCHAIN_HASHES, KNOWN_TXS, KNOWN_BLOCKS = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 , 11

# Conns
LAST_PONG, QUEUED_INVS = 0, 1

# Inventory types
MSG_TX, MSG_BLOCK = 0, 1

# used only to check if the node is already in the network (simulator)
# nodes don't have an overview of the network
NETWORK_NODES = []
REAL_BLOCKCHAIN = []
REPEATED_BLOCK_COUNT = []
TX_NUMBER = 0
BLOCK_NUMBER = 1
LAST_CYCLE_TIME = 999999999999

def init():
	global nodeState

	for nodeId in nodeState:
		REPEATED_BLOCK_COUNT.append({})
		sim.schedulleExecution(CYCLE, nodeId)

def improve_performance(cycle):
	global LAST_CYCLE_TIME

	if time.time() - LAST_CYCLE_TIME > 2:
		LAST_CYCLE_TIME = time.time()
	else:
		LAST_CYCLE_TIME = time.time()
		return
	#if cycle % 100 != 0 or cycle == 0:
	#	return

	# remove half the messages
	del sim.queue[:int(len(sim.queue)/2)]
	gc.collect()
	if gc.garbage:
		gc.garbage[0].set_next(None)
		del gc.garbage[:]

def CYCLE(self):
	global nodeState, TX_NUMBER, BLOCK_NUMBER

	if self == 0:
		logger.info('node: {} cycle: {}'.format(self, nodeState[self][CURRENT_CYCLE]))
		print("Cycle: ", nodeState[self][CURRENT_CYCLE], "/", nbCycles-1)
		print("Events: ", sim.getNumberEvents())

	if self not in nodeState:
		return

	if self not in NETWORK_NODES:
		# join the network
		if random.random() <= probJoin:
			join(self)
	else:
		# lookup for new neighbors
		if connsCount(self) == 0:
			join(self)
		elif connsCount(self) <= p:
			lookup(self)

		# lifecheck and cycle ping
		lifeCheckDBNeighbs(self)
		for n in nodeState[self][CONNS]:
			sim.send(PING, n, self, PING_MSG)
			#nodeState[self][MEMB_MSGS_SENT] += 1

			# Announcement
			inv = nodeState[self][CONNS][n][QUEUED_INVS]
			if len(inv) > 0:
				sim.send(INV, n, self, INV_MSG, inv)
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

		# Create transactions
		# Stop creating txs if there are lots of events
		if sim.getNumberEvents() < 5000 and random.random() <= probTxCreate:
			for _ in range(0, 1+int(random.random()*maxTxCreate)):
				t = generateTx(TX_NUMBER)
				TX_NUMBER += 1
				nodeState[self][KNOWN_TXS][t.getHash()] = t
				addInvConns(self, Inventory(MSG_TX, t.getHash()))

		# Create block
		# Stop creating blocks at 90% of cycles
		if nodeState[self][CURRENT_CYCLE] < 0.9 * nbCycles:
			if len(nodeState[self][KNOWN_TXS]) > minTxPerBlock and self < miners:
				b = generateBlock(self, nodeState[self][KNOWN_TXS].values())
				REPEATED_BLOCK_COUNT[self].update({b.getHash():0})
				BLOCK_NUMBER += 1
				nodeState[self][KNOWN_TXS].clear()
				nodeState[self][KNOWN_BLOCKS][b.getHash()] = b
				addInvConns(self, Inventory(MSG_BLOCK, b.getHash()))

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

	addConn(self, destNode)
	sim.send(VERSION, destNode, self, VERSION_MSG)
	sim.send(GETADDR, destNode, self, GETADDR_MSG)
	sim.send(GETHEADERS, destNode, self, GETHEADERS_MSG, nodeState[self][BLOCKCHAIN][0].getHash(), 0)
	nodeState[self][MEMB_MSGS_SENT] += 2
	nodeState[self][DISS_MSGS_SENT] += 1

def lookup(self):
	for n in list(nodeState[self][CONNS].keys()):
		sim.send(ADDR, n, self, ADDR_MSG, [self])
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

# Membership messages

def PING(self, source, msg):
	#nodeState[self][MEMB_MSGS_RECEIVED] += 1

	sim.send(PONG, source, self, PONG_MSG)
	#nodeState[self][MEMB_MSGS_SENT] += 1


def PONG(self, source, msg):
	#nodeState[self][MEMB_MSGS_RECEIVED] += 1

	if source not in nodeState[self][CONNS]:
		return
	updateEntryPong(self, source, nodeState[self][CURRENT_TIME])


def VERSION(self, source, msg):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][MEMB_MSGS_RECEIVED] += 1

	if self == source:
		return

	sim.send(VERACK, source, self, VERACK_MSG)
	nodeState[self][MEMB_MSGS_SENT] += 1


def VERACK(self, source, msg):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][MEMB_MSGS_RECEIVED] += 1


def GETADDR(self, source, msg):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][MEMB_MSGS_RECEIVED] += 1

	sim.send(ADDR, source, self, ADDR_MSG, createSample(self))
	nodeState[self][MEMB_MSGS_SENT] += 1


def ADDR(self, source, msg, addrs):
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][MEMB_MSGS_RECEIVED] += 1

	temp = []
	for n in addrs:
		if n != self and n not in nodeState[self][CONNS]:
			temp.append(n)

	for n in temp:
		addConn(self, n)
		sim.send(VERSION, n, self, VERSION_MSG)
		nodeState[self][MEMB_MSGS_SENT] += 1
	
	#for n in random.sample(list(nodeState[self][CONNS].keys()), min(connsCount(self), 2)):
	#	sim.send(ADDR, source, self, ADDR_MSG, addrs)
	#	nodeState[self][MSGS_SENT] += 1


# Message Dissemination Messages

def INV(self, source, msg, inv):
	# The inv message (inventory message) transmits one or more inventories of objects known to the transmitting peer.
	# It can be sent unsolicited to announce new transactions or blocks, or it can be sent in reply to a getblocks message or mempool message.
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	tmp = []
	for i in inv:
		if i.getType() == MSG_TX:
			if i.getHash() not in nodeState[self][KNOWN_TXS]:
				tmp.append(i)
		elif i.getType() == MSG_BLOCK:
			if i.getHash() not in nodeState[self][KNOWN_BLOCKS]:
				tmp.append(i)
	
	if len(tmp) == 0:
		return
	sim.send(GETDATA, source, self, GETDATA_MSG, tmp)
	nodeState[self][DISS_MSGS_SENT] += 1


def GETHEADERS(self, source, msg, start, end):
	# The getheaders message requests a headers message that provides block headers starting from a particular point in the block chain.
	# It allows a peer which has been disconnected or started for the first time to get the headers it hasn’t seen yet.
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	tmp = []
	started = False
	for b in nodeState[self][BLOCKCHAIN]:
		if b.getHash() == end:
			tmp.append(b.getHeader())
			break
		if started:
			tmp.append(b.getHeader())
		elif not started and b.getHash() == start:
			started = True
			tmp.append(b.getHeader())
		else:
			continue

	sim.send(HEADERS, source, self, HEADERS_MSG, tmp)
	nodeState[self][DISS_MSGS_SENT] += 1


def HEADERS(self, source, msg, headers):
	# The headers message sends block headers to a node which previously requested certain headers with a getheaders message.
	# A headers message can be empty.
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	if verifyHeaders(self, headers):
		sim.send(GETBLOCKS, source, self, GETBLOCKS_MSG, headers)
		nodeState[self][DISS_MSGS_SENT] += 1


def GETBLOCKS(self, source, msg, headers):
	# The getblocks message requests an inv message that provides block header hashes starting from a particular point in the block chain.
	# It allows a peer which has been disconnected or started for the first time to get the data it needs to request the blocks it hasn’t seen.
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	tmp = []
	for b in nodeState[self][BLOCKCHAIN]:
		if b.getHeader() in headers:
			tmp.append(Inventory(MSG_BLOCK, b.getHash()))

	sim.send(INV, source, self, INV_MSG, tmp)
	nodeState[self][DISS_MSGS_SENT] += 1


def GETDATA(self, source, msg, inv):
	# The getdata message requests one or more data objects from another node.
	# The objects are requested by an inventory, which the requesting node typically received previously by way of an inv message.
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	for i in inv:
		if i.getType() == MSG_TX:
			t = nodeState[self][KNOWN_TXS].get(i.getHash())
			if t is None:
				continue
			sim.send(TX, source, self, TX_MSG, t)
			nodeState[self][DISS_MSGS_SENT] += 1
		elif i.getType() == MSG_BLOCK:
			tmp = nodeState[self][BLOCKCHAIN_HASHES].copy()
			tmp.update(nodeState[self][KNOWN_BLOCKS])
			b = tmp.get(i.getHash())
			if b is None:
				continue
			sim.send(BLOCK, source, self, BLOCK_MSG, b)
			nodeState[self][DISS_MSGS_SENT] += 1


def BLOCK(self, source, msg, block):
	# The block message transmits a single serialized block in the format described in the serialized blocks section.
	# 1 - GetData Response: Nodes will always send it in response to a getdata message that requests the block with an inventory type of MSG_BLOCK (provided the node has that block available for relay).
	# 2 - Unsolicited: Some miners will send unsolicited block messages broadcasting their newly-mined blocks to all of their peers.
	# Many mining pools do the same thing, although some may be misconfigured to send the block from multiple nodes, possibly sending the same block to some peers more than once.
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1
	
	if block.getHash() not in REPEATED_BLOCK_COUNT[self]:
		REPEATED_BLOCK_COUNT[self].update({block.getHash():1})
	else:
		REPEATED_BLOCK_COUNT[self][block.getHash()] += 1

	if verifyBlocks(self, block):
		if block.getHash() not in nodeState[self][KNOWN_BLOCKS]:
			nodeState[self][KNOWN_BLOCKS][block.getHash()] = block
			addInvConns(self, Inventory(MSG_BLOCK, block.getHash()))
		rmInvConn(self, source, MSG_BLOCK, block.getHash())


def TX(self, source, msg, tx):
	# The tx message transmits a single transaction in the raw transaction format
	# Bitcoin Core and BitcoinJ will send it in response to a getdata message that requests the transaction with an inventory type of MSG_TX.
	# Observation: BitcoinJ will send a tx message unsolicited for transactions it originates. (not Bitcoin Core)
	logger.info("Node: {} Received: {} From: {}".format(self, msg, source))
	nodeState[self][DISS_MSGS_RECEIVED] += 1

	if verifyTxs(self, tx):
		if tx.getHash() not in nodeState[self][KNOWN_TXS]:
			nodeState[self][KNOWN_TXS][tx.getHash()] = tx
			addInvConns(self, Inventory(MSG_TX, tx.getHash()))
		rmInvConn(self, source, MSG_TX, tx.getHash())


# Node functions

def createNode(id):
	# CURRENT_CYCLE			# simulator
	# CURRENT_TIME			# simulator
	# MEMB_MSGS_RECEIVED	# stats
	# MEMB_MSGS_SENT		# stats
	# DISS_MSGS_RECEIVED	# stats
	# DISS_MSGS_SENT		# stats
	# ID					# public
	# CONNS					# private
	# BLOCKCHAIN			# private
	# BLOCKCHAIN_HASHES		# private
	# KNOWN_TXS				# private
	# KNOWN_BLOCKS			# private

	node = [0, 0, 0, 0, 0, 0, id, dict(), [], dict(), dict(), dict()]

	return node


# Neighbs
# [node] (lastPong)

def addConn(self, node):
	if self == node:
		return
	nodeState[self][CONNS][node] = [nodeState[self][CURRENT_TIME], []]

def connsCount(self):
	return len(nodeState[self][CONNS])

def addInvConns(self, inv):
	for n in nodeState[self][CONNS]:
		nodeState[self][CONNS][n][QUEUED_INVS].append(inv)

def rmInvConn(self, node, type, hash):
	if node not in nodeState[self][CONNS]:
		return
	for i in nodeState[self][CONNS][node][QUEUED_INVS]:
		if type == i.getType() and hash == i.getHash():
			nodeState[self][CONNS][node][QUEUED_INVS].remove(i)

def updateEntryPong(self, node, time):
	nodeState[self][CONNS][node][LAST_PONG] = time

def createSample(self):
	temp = list(nodeState[self][CONNS].keys())
	sample = []

	while temp and len(sample) < sampleSize:
		n = random.choice(temp)
		sample.append(n)
		temp.remove(n)
	
	return sample

def lifeCheckDBNeighbs(self):
	# Cycles of 1sec 60*90 = 90min
	for n in nodeState[self][CONNS]:
		if nodeState[self][CURRENT_TIME] - nodeState[self][CONNS][n][LAST_PONG] > 60 * 90:
			del nodeState[self][CONNS][n]

# Blockchain methods
def generateGenesisBlock():
	header = ("0", "0", 0)
	body = ("0", [])
	return Block(header, body)

def generateBlock(self, txs):
	# Simplified version without extra stuff
	# Header (prev_hash, merkle_root, timestamp)
	# Body (merkle_proof(0), txs)

	mt = merkletools.MerkleTools(hash_type="sha256")
	for t in txs:
		mt.add_leaf(t.getHash(), True)
	mt.make_tree()

	header = (nodeState[self][BLOCKCHAIN][-1].getHash(), mt.get_merkle_root(), nodeState[self][CURRENT_TIME])
	body = (mt.get_proof(0), txs)
	block = Block(header, body)
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
	return True

def verifyBlocks(self, blocks):
	return True

def verifyTxs(self, txs):
	return True

# Block definition
class Block:
	def __init__(self, header, body):
		# Simplified version without extra stuff
		# Header (prev_hash, merkle_root, timestamp)
		# Body (merkle_proof(0), txs)
		self.nonce = random.random() * 10000000
		self.header = header
		self.body = body
		#sha256(prev_hash, merkle_root, timestamp)
		h = hashlib.sha256()
		h.update(self.header[0].encode('utf-8'))
		h.update(self.header[1].encode('utf-8'))
		h.update(str(self.header[2]).encode('utf-8'))
		self.hash = h.hexdigest()

	def __str__(self):
		return self.getHash()

	def __eq__(self, other):
		if self.nonce == other.nonce:
			return 0
		elif self.nonce < other.nonce:
			return -1
		else:
			return 1

	def getHeader(self):
		return self.header

	def getHash(self):
		return self.hash

	def getBody(self):
		return self.body


# Transaction definition
class Tx:
	def __init__(self, n):
		self.nonce = random.random() * 10000000
		self.n = n
		self.hash = hashlib.sha256(str(self.n).encode('utf-8')).hexdigest()

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
		return self.hash


# Inventory definition
class Inventory:
	def __init__(self, type, hash):
		self.nonce = random.random() * 10000000
		self.type = type
		self.hash = hash

	def __str__(self):
		return self.hash
	
	def __eq__(self, other):
		if self.nonce == other.nonce:
			return 0
		elif self.nonce < other.nonce:
			return -1
		else:
			return 1

	def getType(self):
		return self.type

	def getHash(self):
		return self.hash


# Simulator
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
	totalMsgsReceived = list(map(lambda x: nodeState[x][MEMB_MSGS_RECEIVED] + nodeState[x][DISS_MSGS_RECEIVED], nodeState))
	totalMsgsSent =  list(map(lambda x: nodeState[x][MEMB_MSGS_SENT] + nodeState[x][DISS_MSGS_SENT], nodeState))
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
			conns_bound[n] = [0, connsCount(n)]
		#blockchain_local = []
		#known_blocks = []
		#known_txs = []
		lat_sum = 0

		bc_right = True
		for b in REAL_BLOCKCHAIN:
			if b.getHash() not in nodeState[n][BLOCKCHAIN_HASHES] and b.getHash() not in nodeState[n][KNOWN_BLOCKS]:
				bc_right = False
				bc_wrong += 1
				break
		bc_right = True
		for b in REAL_BLOCKCHAIN:
			if b not in nodeState[n][BLOCKCHAIN]:
				bc_right = False
				bc_wrong += 1
				break

		#for _, b in nodeState[n][KNOWN_BLOCKS].items():
		#	known_blocks.append(b.getHash())
		#for _, t in nodeState[n][KNOWN_TXS].items():
		#	known_txs.append(t.getHash())

		for c in nodeState[n][CONNS].keys():
			lat_sum += sim.getMessageLatency(n, c)
			lat_out_sum += sim.getMessageLatency(n, c)
			lat_in_sum += sim.getMessageLatency(c, n)
			if c not in conns_bound:
				conns_bound[c] = [1, connsCount(c)]
			else:
				conns_bound[c][0] += 1

		avg_latency = 0
		if connsCount(n) != 0:
			avg_latency = lat_sum/connsCount(n)
		data['nodes'].append({
			"id": nodeState[n][ID],
			"memb_msgs_received": nodeState[n][MEMB_MSGS_RECEIVED],
			"diss_msgs_received": nodeState[n][DISS_MSGS_RECEIVED],
			"memb_msgs_sent": nodeState[n][MEMB_MSGS_SENT],
			"diss_msgs_sent": nodeState[n][DISS_MSGS_SENT],
			"peer_avg_latency": "%0.1f ms" % (avg_latency),
			"blockchain_right": bc_right,
			"blockchain_len": len(nodeState[n][BLOCKCHAIN]),
			"known_blocks_len": len(nodeState[n][KNOWN_BLOCKS]),
			#"blockchain_local": blockchain_local,
			#"known_blocks": known_blocks,
			#"known_txs": known_txs,
			#"outbound_conns": list(nodeState[n][CONNS].keys()),
		})

	#blockchain = []
	#for b in REAL_BLOCKCHAIN:
	#	blockchain.append({
	#		"prev_hash": b.getHeader()[0],
	#		"hash": b.getHash(),
	#	})

	for n in conns_bound:
		data['nodes'][n].update({
			"inbound_count": conns_bound[n][0],
			"outbound_count": conns_bound[n][1]
		})

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
		"blockchain_len": len(REAL_BLOCKCHAIN),
		"total_blocks": BLOCK_NUMBER,
		"total_txs": TX_NUMBER,
		#"blockchain": blockchain,
	})

	with open(dir+".repeated", 'w+') as outfile: json.dump(REPEATED_BLOCK_COUNT, outfile, indent=2)
	with open(dir, 'w+') as outfile: json.dump(data, outfile, indent=2)

def configure(config):
	global nbNodes, nbCycles, probJoin, nodeState, nodeCycle, p, sampleSize, miners, minTxPerBlock, probTxCreate, maxTxCreate

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

	p = config['p']
	sampleSize = config['sampleSize']
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

	if len(sys.argv) < 2:
		logger.error("Invocation: ./btc_memb.py <conf_out_dir>")
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
