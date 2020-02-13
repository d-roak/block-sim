# Misc. stats and plotting utils.
# Miguel Matos - mm@gsd.inesc-id.pt
# (c) 2012-2017

import math
import random
import pickle

#original forom scipy, adjusted to run on pypy
def scoreatpercentile(a, per, limit=(),isSorted=False):
    #values = np.sort(a,axis=0) #not yet implemented in pypy
    if not isSorted:
	    values = sorted(a) #np.sort(a,axis=0)
    else:
    	values = a


    if limit:
        values = values[(limit[0] <= values) & (values <= limit[1])]

    #idx = per /100. * (values.shape[0] - 1)
    idx = per /100. * (len(values)  - 1)
    if (idx % 1 == 0):
        return values[int(idx)]
    else:
        return _interpolate(values[int(idx)], values[int(idx) + 1], idx % 1)


def _interpolate(a, b, fraction):
    """Returns the point at the given fraction between a and b, where
    'fraction' must be between 0 and 1.
    """
    return a + (b - a)*fraction

def percentiles(data,percs=[0,1,5,25,50,75,95,99,100],paired=True,roundPlaces=None):
	"""
	Returns the values at the given percentiles. 
	Inf percs is null gives the 5,25,50,75,95,99,100 percentiles.
	"""

	data = sorted(data)
	#data migth be an iterator so we need to do this check after sorting
	if len(data) == 0:
		return []
	result = []

	for p in percs:
		score = scoreatpercentile(data,p,isSorted=True)
		if roundPlaces:
			score = round(score,roundPlaces)
		if paired:
			result.append( (p, score))
		else:
			result.append( score)


	return result

def checkLatencyNodes(latencyTable, nbNodes, defaultLatency=None):
    global latencyValue

    if latencyTable == None and defaultLatency != None:
        print('WARNING: using constant latency')
        latencyTable = {n: {m: defaultLatency for m in range(nbNodes)} for n in range(nbNodes)}
        # latencyTable = {n : {m: random.randint(0,defaultLatency)for m in range(nbNodes)} for n in range(nbNodes) }
        return latencyTable

    nbNodesAvailable = len(latencyTable)

    latencyList = [l for tmp in latencyTable.itervalues() for l in tmp.values()]
    latencyValue = math.ceil(percentiles(latencyList, percs=[50], paired=False)[0])

    if nbNodes > nbNodesAvailable:
        nodeIds = range(nbNodes)

        #logger.warning('Need to add nodes to latencyTable')
        for node in range(nbNodesAvailable):
            latencyTable[node].update({target: random.choice(latencyList) for target in nodeIds[nbNodesAvailable:]})

        for node in range(nbNodesAvailable, nbNodes):
            latencyTable[node] = {target: random.choice(latencyList) for target in nodeIds}
            latencyTable[node].pop(node)  # remove itself
        # FIXME: we should also remove some other nodes to be more faithful to the original distribution

        with open('/tmp/latencyTable.obj', 'w') as f:
            pickle.dump(latencyTable, f)

    return latencyTable


