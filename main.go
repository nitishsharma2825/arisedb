package main

import (
	"bytes"
	"encoding/binary"
	"log"
)

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf node with values
)

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		log.Fatal("Size exceeded max page size")
	}
}

type BNode []byte // can be dumped to the disk

type Btree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) []byte // dereference a pointer
	new func([]byte) uint64 // allocate a page
	del func(uint64)        // deallocate a page
}

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	if idx > node.nkeys() {
		log.Fatal("Index is greater than no of keys in node")
	}
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getValue(idx uint16) []byte {
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// returns the first kid node index whose range intersects the key. (kid[i] <= key)
// TODO: binary search
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	// the first key is a copy from the parent node,
	// thus it's always less than or equal to the key
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp > 0 {
			break
		}
	}
	return found
}

// add a new key to a leaf node
// get insert position using nodeLookupLE
// copy everything to a new node with extra key (COW)
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	// set the header for the new node
	new.setHeader(BNODE_LEAF, old.nkeys())

	// copy all existing k-v pairs from old node to the new node
	nodeAppendRange(new, old, 0, 0, idx) // copy keys before updated key

	// Update the specific k-v pair at index idx
	nodeAppendKV(new, idx, old.getPtr(idx), key, val)

	// copy remaining k-v from old node after the updated key
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
}

// copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key))+uint16(len(val)))
}

// copy multiple KVs into the position from old node
func nodeAppendRange(
	new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16,
) {
	for i := uint16(0); i < n; i++ {
		// calculate the index in the old node
		oldIdx := srcOld + i
		// Get the key/value from old node
		key := old.getKey(oldIdx)
		value := old.getValue(oldIdx)

		// Append the k-v pair to the new node
		nodeAppendKV(new, dstNew+i, 0, key, value)
	}
}

// replace a link with one or multiple links
func nodeReplaceKidN(
	tree *Btree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// split a oversized node into 2 so that the 2nd node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {
	// determine the no of keys in the old node
	nkeys := old.nkeys()

	// calculate the midpoint for splitting
	mid := nkeys / 2

	// set up the left node with 1st half of the keys
	left.setHeader(old.btype(), mid)
	for i := uint16(0); i < mid; i++ {
		key := old.getKey(i)
		val := old.getValue(i)
		ptr := old.getPtr(i)
		nodeAppendKV(left, i, ptr, key, val)
	}

	// set up the right node with 2nd half of the keys
	right.setHeader(old.btype(), nkeys-mid)
	for i := mid; i < nkeys; i++ {
		key := old.getKey(i)
		val := old.getValue(i)
		ptr := old.getPtr(i)
		nodeAppendKV(right, i-mid, ptr, key, val)
	}
}

// split a node if its too big, the results are 1-3 nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 nodes
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	return 3, [3]BNode{leftleft, middle, right}
}

// insert a KV into a node, the result might be split
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes
func treeInsert(tree *Btree, node BNode, key []byte, val []byte) BNode {
	// the result node
	// its allowed to be bigger than 1 page and will be split if so
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad mode!")
	}

	return new
}

func nodeInsert(
	tree *Btree, new BNode, node BNode, idx uint16,
	key []byte, val []byte,
) {
	kptr := node.getPtr(idx)
	// recursive insertion to the kid node
	knode := treeInsert(tree, tree.get(kptr), key, val)
	// split the result
	nsplit, split := nodeSplit3(knode)
	// deallocate the kid node
	tree.del(kptr)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
}

func (tree *Btree) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		// create the first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := treeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		// the root was split, add a new level
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)

	// copy all keys and value before the index
	nodeAppendRange(new, old, 0, 0, idx)

	// skip the key at idx (the one to be deleted)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx-1)
}

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())

	for i := uint16(0); i < left.nkeys(); i++ {
		key := left.getKey(i)
		val := left.getValue(i)
		ptr := left.getPtr(i)
		nodeAppendKV(new, i, ptr, key, val)
	}

	for i := uint16(0); i < right.nkeys(); i++ {
		key := left.getKey(i)
		val := left.getValue(i)
		ptr := left.getPtr(i)
		nodeAppendKV(new, i, ptr, key, val)
	}
}

// replace 2 adjacent links with 1
func nodeReplace2Kid(
	new BNode, old BNode, idx uint16, ptr uint64, key []byte,
) {
	new.setHeader(old.btype(), old.nkeys()-1)

	// copy all existing keys and pointers before idx
	nodeAppendRange(new, old, 0, 0, idx)

	// append merged pointer at index idx
	new.setPtr(idx, ptr)

	// copy remaining keys and pointers after idx + 1
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

// should the updated kid be merged with a sibling?
func shouldMerge(
	tree *Btree, node BNode,
	idx uint16, updated BNode,
) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := BNode(tree.get(node.getPtr(idx - 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}

	if idx+1 < node.nkeys() {
		sibling := BNode(tree.get(node.getPtr(idx + 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}
	return 0, BNode{}
}

// delete a key from the tree
func treeDelete(tree *Btree, node BNode, key []byte) BNode {
	// find the index where the key should be located
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		// If we are at a leaf node, attempt to delete the key
		if idx < node.nkeys() && bytes.Equal(key, node.getKey(idx)) {
			// key found in leaf, perform deletion
			newLeaf := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // new leaf node for COW
			leafDelete(newLeaf, node, idx)
			return newLeaf
		} else {
			// key not found in this leaf; nothing to delete
			return node
		}
	case BNODE_NODE:
		// If we are at an internal node, descend to the appropriate child
		kptr := node.getPtr(idx)
		childNode := tree.get(kptr)

		// Recursively delete from the child node
		newChild := treeDelete(tree, childNode, key)

		// Check if we need to update the pointer in the parent node
		if len(newChild) != len(childNode) || cap(newChild) != cap(childNode) || &newChild[0] != &childNode[0] {
			// A new child was returned due to deletion
			newInternal := BNode(make([]byte, 2*BTREE_PAGE_SIZE))   // new internal node for COW
			nodeReplaceKidN(tree, newInternal, node, idx, newChild) // update pointer in parent
			return newInternal
		}

		return node
	default:
		panic("bad type")
	}
}

// delete a key from an internal node; part of the treeDelete
func nodeDelete(tree *Btree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr)

	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		new.setHeader(BNODE_NODE, 0)
	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceKidN(tree, new, node, idx, updated)
	}

	return new
}
