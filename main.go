package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"internal/syscall/unix"
	"log"
	"os"
	"path"
	"syscall"
)

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

const DB_SIG = "BuildYourOwnDB06"

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
	get func(uint64) []byte // read a page
	new func([]byte) uint64 // append a page
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

// insert a new key or update an existing key
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

// deleteakeyandreturnswhetherthekeywasthere
func (tree *Btree) Delete(key []byte) bool

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

// KV store with COW B+tree backed by a file
type KV struct {
	Path string // file name
	// internals
	fd   int
	tree Btree
	free FreeList
	mmap struct {
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64          // database size in number of pages
		nappend uint64          // number of pages to be appended
		updates map[uint64]byte // pending updates, including appended pages
	}
	failed bool // Did the last update failed?
}

// Btree.get, read a page
func (db *KV) pageRead(ptr uint64) []byte {
	if node, ok := db.page.updates[ptr]; ok {
		return node
	}
	return db.pageReadFile(ptr)
}

func writePages(db *KV) error {
	// extend the mmap if needed
	size := (int(db.page.flushed) + len(db.page.temp)) * BTREE_PAGE_SIZE
	if err := extendMmap(db, size); err != nil {
		return err
	}
	// write data pages to the file
	offset := int64(db.page.flushed * BTREE_PAGE_SIZE)
	if _, err := unix.Pwritev(db.fd, db.page.temp, offset); err != nil {
		return err
	}
	// discard in-memory data
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}

func (db *KV) pageAppend(node []byte) uint64 {
	ptr := db.page.flushed + uint64(len(db.page.temp)) // just append
	db.page.temp = append(db.page.temp, node)
	return ptr
}

func (db *KV) Open() error {
	db.tree.get = db.pageRead      // read a page
	db.tree.new = db.pageAlloc     // reuse from the free list or append
	db.tree.del = db.free.PushTail // freed pages go to the free list
	// free list callbacks
	db.free.get = db.pageRead   // read a page
	db.free.new = db.pageAppend // append a page
	db.free.set = db.pageWrite  // in-place update
	return nil
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key []byte, val []byte) error {
	meta := saveMeta(db) // save the in memory state
	db.tree.Insert(key, val)
	return updateOrRevert(db, meta)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, updateFile(db)
}

func updateFile(db *KV) error {
	// 1. Write new nodes
	if err := writePages(db); err != nil {
		return err
	}
	// 2. fsync to enforce the order between 1 and 3
	if err := syscall.Fsync(db.fd); err != nil {
		return err
	}
	// 3. Update the root pointer atomically
	if err := updateRoot(db); err != nil {
		return err
	}
	// 4. fsync to make everything persistent
	syscall.Fsync(db.fd)

	// prepare the free list for the next update
	db.free.SetMaxSeq()
	return nil
}

func createFileSync(file string) (int, error) {
	// obtain the directory fd
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirfd, err := syscall.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}
	defer syscall.Close(dirfd)

	// open or create the file
	flags = os.O_RDWR | os.O_CREATE
	fd, err := syscall.Openat(dirfd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}

	// fsync the directory
	if err = syscall.Fsync(dirfd); err != nil {
		_ = syscall.Close(fd) // may leave an empty file
		return -1, fmt.Errorf("fsync directory: %w", err)
	}

	return fd, nil
}

func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil // enough range
	}
	alloc := max(db.mmap.total, 64<<20) // double the current address space
	for db.mmap.total+alloc < size {
		alloc *= 2 // still not enough?
	}
	chunk, err := syscall.Mmap(
		db.fd, int64(db.mmap.total), alloc,
		syscall.PROT_READ, syscall.MAP_SHARED, // read-only
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

// |sig|root_ptr|page_used|head_page|head_seq|tail_page|tail_seq|
//
// |16B| 8B | 8B | 8B | 8B | 8B | 8B |
func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

func loadMeta(db *KV, data []byte) {
	// Load the root pointer
	db.tree.root = binary.NativeEndian.Uint64(data[16:24])
	// Load the page flushed value
	db.page.flushed = binary.NativeEndian.Uint64(data[24:32])
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize == 0 { // empty file
		// reserve 2 pages; the meta page and a free list node
		db.page.flushed = 2
		// add an initial node to the free list so its never empty
		db.free.headPage = 1 // 2nd page
		db.free.tailPage = 1
		return nil
	}
	// read the page
	data := db.mmap.chunks[0]
	loadMeta(db, data)
	// verify the page
	// ...
	return nil
}

// Update the meta page. it must be atomic
func updateRoot(db *KV) error {
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("write meta page: %w", err)
	}
	return nil
}

func updateOrRevert(db *KV, meta []byte) error {
	// ensure the on-disk meta page matches the in-memory one after an error
	if db.failed {
		// write the sync the previous meta page
		db.failed = false
	}
	// 2-phase update
	err := updateFile(db)
	// revert an error
	if err != nil {
		// the on-disk meta page is in unknown state
		// mark it to be rewritten on later recovery.
		db.failed = true
		// the in-memory states can be reverted immediately to allow reads
		loadMeta(db, meta)
		// discard temps
		db.page.temp = db.page.temp[:0]
	}
	return err
}

// node foramt:
type LNode []byte

const FREE_LIST_HEADER = 8
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

// getters and setters
func (node LNode) getNext() uint64
func (node LNode) setNext(next uint64)
func (node LNode) getPtr(idx int) uint64
func (node LNode) setPtr(idx int, ptr uint64)

type FreeList struct {
	// callbacks for managing on-disk pages
	get func(uint64) []byte // read a page
	new func([]byte) uint64 // append a new page
	set func(uint64) []byte // update an existing page
	// persisted data in the meta page
	headPage uint64 // pointer to the list head node
	headSeq  uint64 // monotonic seq number to index into the list head
	tailPage uint64
	tailSeq  uint64 // monotonic seq number to index into the list tail
	// in-memory states
	maxSeq uint64 // saved 'tailSeq' to prevent consuming newly added items
}

// get 1 item from the list head. return 0 on failure
func (f1 *FreeList) PopHead() uint64

// add 1 item to the tail
func (fl *FreeList) PushTail(ptr uint64) {
	// add it to the tail node
	LNode(fl.set(fl.tailPage)).setPtr(seq2idx(fl.tailSeq), ptr)
	fl.tailSeq++
	// add a new tail node if its full
	if seq2idx(fl.tailSeq) == 0 {
		// try to reuse from the list head
		next, head := flPop(fl)
		if next == 0 {
			// or allocate a new node by appending
			next = fl.new(make([]byte, BTREE_PAGE_SIZE))
		}
		// link to the new tail node
		LNode(fl.set(fl.tailPage)).setNext(next)
		fl.tailPage = next
		// also add the head node if its removed
		if head != 0 {
			LNode(fl.set(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}

func seq2idx(seq uint64) int {
	return int(seq % FREE_LIST_CAP)
}

func (fl *FreeList) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

// remove 1 item from the head node, and remove the head node if empty
func flPop(fl *FreeList) (ptr uint64, head uint64) {
	if fl.headSeq == fl.maxSeq {
		return 0, 0 // can't advance
	}

	node := LNode(fl.get(fl.headPage))
	ptr = node.getPtr(seq2idx(fl.headSeq))
	fl.headSeq++
	// move to the next one if the head node is empty
	if seq2idx(fl.headSeq) == 0 {
		head, fl.headPage = fl.headPage, node.getNext()
	}
	return
}

// get 1 item from the list head. return 0 on failure
func (fl *FreeList) PopHead() uint64 {
	ptr, head := flPop(fl)
	if head != 0 { // the empty head node is recycled
		fl.PushTail(head)
	}
	return ptr
}

func (db *KV) pageAlloc(node []byte) uint64 {
	if ptr := db.free.PopHead(); ptr != 0 {
		db.page.updates[ptr] = node
		return ptr
	}
	return db.pageAppend(node)
}

func (db *KV) pageWrite(ptr uint64) []byte {
	if node, ok := db.page.updates[ptr]; ok {
		return node
	}
	node := make([]byte, BTREE_PAGE_SIZE)
	copy(node, db.pageReadFile(ptr))
	db.page.updates[ptr] = node
	return node
}

func (db *KV) pageReadFile(ptr uint64) []byte {
	// same as KV.pageRead
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE
		if ptr < end {
			offset := BTREE_PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+BTREE_PAGE_SIZE]
		}
		start = end
	}
	panic("bad ptr")
}

const (
	TYPE_BYTES = 1 // string (of arbitrary bytes)
	TYPE_INT64 = 2 // integer; 64 bit signed
)

// table cell
type Value struct {
	Type uint32 // tagged union
	I64  int64
	Str  []byte
}

// table row
type Record struct {
	Cols []string
	Vals []Value
}

func (rec *Record) AddStr(col string, val []byte) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_BYTES, Str: val})
	return rec
}

func (rec *Record) AddInt64(col string, val int64) *Record {
	rec.Cols = append(rec.Cols, col)
	rec.Vals = append(rec.Vals, Value{Type: TYPE_INT64, I64: val})
	return rec
}
func (rec *Record) Get(col string) *Value {
	idxVal := 0
	for idx, val := range rec.Cols {
		if val == col {
			idxVal = idx
			break
		}
	}
	return &rec.Vals[idxVal]
}

type TableDef struct {
	// user defined
	Name  string
	Types []uint32 // col types
	Cols  []string // col names
	PKeys int      // the first `PKeys` columns are the primary key
	// auto-assigned B-tree key prefixes for different tables
	Prefix uint32
}

var TDEF_TABLE = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

var TDEF_META = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

// get a single row by the PK
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

func (db *DB) Insert(tablestring, recRecord) (bool, error)
func (db *DB) Update(tablestring, recRecord) (bool, error)
func (db *DB) Upsert(tablestring, recRecord) (bool, error)
func (db *DB) Delete(tablestring, recRecord) (bool, error)

type DB struct {
	Path string
	kv   KV
}

// get a single row by the primary key
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	// 1. reorder the input cols according to the schema
	values, err := checkRecord(tdef, *rec, tdef.PKeys)
	if err != nil {
		return false, err
	}
	// 2. encode the primary key
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	// 3. query the KV store
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}
	// 4. decode the value into columns
	for i := tdef.PKeys; i < len(tdef.Cols); i++ {
		values[i].Type = tdef.Types[i]
	}
	decodeValues(val, values[tdef.PKeys:])
	rec.Cols = tdef.Cols
	rec.Vals = values
	return true, nil
}

// reorder a record and check for missing cols
// n == tdef.Pkeys: record is exactly a PK
// n == len(tdef.Cols): record contains all cols
func checkRecord(tdef *TableDef, rec Record, n int) ([]Value, error)

// encode cols for the "key" of the KV
func encodeKey(out []byte, prefix uint32, vals []Value) []byte

// decode columns from the "value" of the KV
func decodeValues(in []byte, out []Value)

func getTableDef(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := dbGet(db, TDEF_TABLE, rec)
	if !ok {
		return nil
	}

	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	return tdef
}

const (
	MODE_UPSERT      = 0 // insert or replace
	MODE_UPDATE_ONLY = 1 // update existing keys
	MODE_INSERT_ONLY = 2 // only add new keys
)

type UpdateReq struct {
	tree *Btree
	// out
	Added bool // added a new key
	// in
	Key  []byte
	Val  []byte
	Mode int
}

func (tree *Btree) Update(req *UpdateReq)

func dbUpdate(db *DB, tdef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tdef, rec, len(tdef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tdef.Prefix, values[:tdef.PKeys])
	val := encodeValues(nil, values[tdef.PKeys:])
	return db.kv.Update(key, val, mode)
}

type BIter struct {
	tree *Btree
	path []BNode // from root to leaf
	pos  []uint16
}

// find the closest position that is less or equal to the input key
func (tree *Btree) SeekLE(key []byte) *BIter

// getthecurrentKVpair
func (iter *BIter) Deref() ([]byte, []byte)

// preconditionoftheDeref()
func (iter *BIter) Valid() bool

// movingbackwardandforward
func (iter *BIter) Prev()
func (iter *BIter) Next() {
	iterNext(iter, len(iter.path)-1)
}

func iterNext(iter *BIter, level int) {
	if iter.pos[level]+1 < iter.path[level].nkeys() {
		iter.pos[level]++ // move within this node
	} else if level > 0 {
		iterNext(iter, level-1) // move to a sibling node
	} else {
		iter.pos[len(iter.pos)-1]++ // past the last key
		return
	}

	if level+1 < len(iter.pos) { // update the child node
		node := iter.path[level]
		kid := BNode(iter.tree.get(node.getPtr(iter.pos[level])))
		iter.path[level+1] = kid
		iter.pos[level+1] = 0
	}
}

func (tree *Btree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.root; ptr != 0; {
		node := tree.get(ptr)
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		ptr = node.getPtr(idx)
	}
	return iter
}

const (
	CMP_GE = +3 // >=
	CMP_GT = +2 // >
	CMP_LT = -2 // <
	CMP_LE = -3 // <=
)

func (tree *Btree) Seek(key []byte, cmp int) *BIter

// Scanner is a wrapper of B+Tree iterator, it decodes KVs into rows
// within the range or not?
func (sc *Scanner) Valid() bool

// move the underlying B-tree iterator
func (sc *Scanner) Next()

// fetch the current row
func (sc *Scanner) Deref(rec *Record)
func (db *DB) Scan(table string, req *Scanner) error

type Scanner struct {
	// the range, from Key1 to Key2
	Cmp1 int // CMP_??
	Cmp2 int
	Key1 Record
	Key2 Record
	// ...
}
