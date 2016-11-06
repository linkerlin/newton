package dht

// DHT routing using a binary tree and no buckets.
//
// Peers have ids of 20-bytes. When looking up an infohash for itself or for a
// remote host, the nodes have to look in its routing table for the closest
// nodes and return them.
//
// The distance between a node and an infohash is the XOR of the respective
// strings. This means that 'sorting' nodes only makes sense with an infohash
// as the pivot. You can't pre-sort nodes in any meaningful way.
//
// Most bittorrent/kademlia DHT implementations use a mix of bit-by-bit
// comparison with the usage of buckets. That works very well. But I wanted to
// try something different, that doesn't use buckets. Buckets have a single id
// and one calculates the distance based on that, speeding up lookups.
//
// I decided to lay out the routing table in a binary tree instead, which is
// more intuitive. At the moment, the implementation is a real tree, not a
// free-list, but it's performing well.
//
// All nodes are inserted in the binary tree, with a fixed height of 160 (20
// bytes). To lookup an infohash, I do an inorder traversal using the infohash
// bit for each level.
//
// In most cases the lookup reaches the bottom of the tree without hitting the
// target infohash, since in the vast majority of the cases it's not in my
// routing table. Then I simply continue the in-order traversal (but then to
// the 'left') and return after I collect the 8 closest nodes.
//
// To speed things up, I keep the tree as short as possible. The path to each
// node is compressed and later uncompressed if a collision happens when
// inserting another node.
//
// I don't know how slow the overall algorithm is compared to a implementation
// that uses buckets, but for what is worth, the routing table lookups don't
// even show on the CPU profiling anymore.

type tree struct {
	zero, one *tree
	value     *peer
}

// Each query returns up to this number of nodes.
const peers = 8

// recursive version of node insertion.
func (n *tree) insert(newPeer *peer) {
	n.put(newPeer, 0)
}

func (n *tree) branchOut(p1, p2 *peer, i int) {
	// Since they are branching out it's guaranteed that no other nodes
	// exist below this branch currently, so just create the respective
	// nodes until their respective bits are different.
	chr := p1.nodeID[i/8]
	bitPos := byte(i % 8)
	bit := (chr << bitPos) & 128

	chr2 := p2.nodeID[i/8]
	bitPos2 := byte(i % 8)
	bit2 := (chr2 << bitPos2) & 128

	if bit != bit2 {
		n.put(p1, i)
		n.put(p2, i)
		return
	}

	// Identical bits.
	if bit != 0 {
		n.one = &tree{}
		n.one.branchOut(p1, p2, i+1)
	} else {
		n.zero = &tree{}
		n.zero.branchOut(p1, p2, i+1)
	}
}

func (n *tree) put(newPeer *peer, i int) {
	if i >= len(newPeer.nodeID)*8 {
		// Replaces the existing value, if any.
		n.value = newPeer
		return
	}

	if n.value != nil {
		if n.value.nodeID == newPeer.nodeID {
			// Replace existing compressed value.
			n.value = newPeer
			return
		}
		// Compression collision. Branch them out.
		old := n.value
		n.value = nil
		n.branchOut(newPeer, old, i)
		return
	}

	chr := newPeer.nodeID[i/8]
	bit := byte(i % 8)
	if (chr<<bit)&128 != 0 {
		if n.one == nil {
			n.one = &tree{value: newPeer}
			return
		}
		n.one.put(newPeer, i+1)
	} else {
		if n.zero == nil {
			n.zero = &tree{value: newPeer}
			return
		}
		n.zero.put(newPeer, i+1)
	}
}

func (n *tree) lookup(nodeID string) []*peer {
	ret := make([]*peer, 0, peers)
	if n == nil || nodeID == "" {
		return nil
	}
	return n.traverse(nodeID, 0, peers, ret)
}

func (n *tree) getPeer(nodeID string) *peer {
	ret := make([]*peer, 0, 1)
	if n == nil || nodeID == "" {
		return nil
	}
	r := n.traverse(nodeID, 0, 1, ret)
	if len(r) == 0 {
		return nil
	}
	return r[0]
}

func (n *tree) traverse(nodeID string, i, count int, ret []*peer) []*peer {
	if n == nil {
		return ret
	}
	if n.value != nil {
		return append(ret, n.value)
	}
	if i >= len(nodeID)*8 {
		return ret
	}
	if len(ret) >= count {
		return ret
	}

	chr := nodeID[i/8]
	bit := byte(i % 8)

	// This is not needed, but it's clearer.
	var left, right *tree
	if (chr<<bit)&128 != 0 {
		left = n.one
		right = n.zero
	} else {
		left = n.zero
		right = n.one
	}

	ret = left.traverse(nodeID, i+1, count, ret)
	if len(ret) >= count {
		return ret
	}
	return right.traverse(nodeID, i+1, count, ret)
}

// cut goes down the tree and deletes the children nodes if all their leaves
// became empty.
func (n *tree) cut(nodeID string, i int) bool {
	if n == nil {
		return true
	}
	if i >= len(nodeID)*8 {
		return true
	}
	chr := nodeID[i/8]
	bit := byte(i % 8)

	if (chr<<bit)&128 != 0 {
		if n.one.cut(nodeID, i+1) {
			n.one = nil
			if n.zero == nil {
				return true
			}
		}
	} else {
		if n.zero.cut(nodeID, i+1) {
			n.zero = nil
			if n.one == nil {
				return true
			}
		}
	}

	return false
}

/*
// Calculates the distance between two hashes. In DHT/Kademlia, "distance" is
// the XOR of the torrent infohash and the peer node ID.  This is slower than
// necessary. Should only be used for displaying friendly messages.
func hashDistance(id1 string, id2 string) (distance string) {
	d := make([]byte, len(id1))
	if len(id1) != len(id2) {
		return ""
	} else {
		for i := 0; i < len(id1); i++ {
			d[i] = id1[i] ^ id2[i]
		}
		return string(d)
	}
	return ""
}*/
