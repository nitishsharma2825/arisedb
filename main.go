package main

import (
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
	if node1max <= BTREE_PAGE_SIZE {
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
