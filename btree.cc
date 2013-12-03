#include <assert.h>
#include "btree.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
 SIZE_T valuesize,
 BufferCache *cache,
 bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
     superblock.info.keysize,
     superblock.info.valuesize,
     buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
     superblock.info.keysize,
     superblock.info.valuesize,
     buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
       superblock.info.keysize,
       superblock.info.valuesize,
       buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
       return rc;
     }

   }
 }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

 return superblock.Unserialize(buffercache,initblock);
}


ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}


ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
  const BTreeOp op,
  const KEY_T &key,
  VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) { //STRUCTURED SO EQUIVALENT KEY VALUES ARE TO THE LEFT
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to 
	// this one, if it exists
       rc=b.GetPtr(offset,ptr);
       if (rc) { return rc; }
       return LookupOrUpdateInternal(ptr,op,key,value);
     }
   }
    // if we got here, we need to go to the next pointer, if it exists
   if (b.info.numkeys>0) { 
    rc=b.GetPtr(b.info.numkeys,ptr);
    if (rc) { return rc; }
    return LookupOrUpdateInternal(ptr,op,key,value);
  } else {
      // There are no keys at all on this node, so nowhere to go
    return ERROR_NONEXISTENT;
  }
  break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
  for (offset=0;offset<b.info.numkeys;offset++) { 
    rc=b.GetKey(offset,testkey);
    if (rc) {  return rc; }
    if (testkey==key) { 
     if (op==BTREE_OP_LOOKUP) { 
       return b.GetVal(offset,value);
     } else { 
	  // BTREE_OP_UPDATE 
      return b.SetVal(offest, value);
	  // WRITE ME
       return ERROR_UNIMPL;
     }
   }
 }
 return ERROR_NONEXISTENT;
 break;
 default:
    // We can't be looking at anything other than a root, internal, or leaf
 return ERROR_INSANE;
 break;
}  

return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
       os << "Interior: ";
     }
     for (offset=0;offset<=b.info.numkeys;offset++) { 
       rc=b.GetPtr(offset,ptr);
       if (rc) { return rc; }
       os << "*" << ptr << " ";
	// Last pointer
       if (offset==b.info.numkeys) break;
       rc=b.GetKey(offset,key);
       if (rc) {  return rc; }
       for (i=0;i<b.info.keysize;i++) { 
         os << key.data[i];
       }
       os << " ";
     }
   }
   break;
   case BTREE_LEAF_NODE:
   if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
   } else {
    os << "Leaf: ";
  }
  for (offset=0;offset<b.info.numkeys;offset++) { 
    if (offset==0) { 
	// special case for first pointer
     rc=b.GetPtr(offset,ptr);
     if (rc) { return rc; }
     if (dt!=BTREE_SORTED_KEYVAL) { 
       os << "*" << ptr << " ";
     }
   }
   if (dt==BTREE_SORTED_KEYVAL) { 
     os << "(";
   }
   rc=b.GetKey(offset,key);
   if (rc) {  return rc; }
   for (i=0;i<b.info.keysize;i++) { 
     os << key.data[i];
   }
   if (dt==BTREE_SORTED_KEYVAL) { 
     os << ",";
   } else {
     os << " ";
   }
   rc=b.GetVal(offset,value);
   if (rc) {  return rc; }
   for (i=0;i<b.info.valuesize;i++) { 
     os << value.data[i];
   }
   if (dt==BTREE_SORTED_KEYVAL) { 
     os << ")\n";
} else {
	os << " ";
}
}
break;
default:
if (dt==BTREE_DEPTH_DOT) { 
  os << "Unknown("<<b.info.nodetype<<")";
} else {
  os << "Unsupported Node Type " << b.info.nodetype ;
}
}
if (dt==BTREE_DEPTH_DOT) { 
  os << "\" ]";
}
return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME

  //This is really a B+ tree (with optional sequentially linked leaf nodes).
  //This means that on every key you split, you need to include that key AGAIN in it's >= diskblock, so that is eventually included in a leaf node with it's key/value pair.
  //This is also what makes deleting a key a nightmare, since you have to get rid of ALL instances of the ky (including the leaf version with the value), and then rebalance.
  // page 636 in the book has good diagrams of this


  //======ALGORITHM======

  //Lookup key
  //If it exists, call update on it with the new value


  return ERROR_UNIMPL;
}

ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, value);
  return ERROR_UNIMPL;
}


ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}


//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
  ostream &o,
  BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
       rc=b.GetPtr(offset,ptr);
       if (rc) { return rc; }
       if (display_type==BTREE_DEPTH_DOT) { 
         o << node << " -> "<<ptr<<";\n";
       }
       rc=DisplayInternal(ptr,o,display_type);
       if (rc) { return rc; }
     }
   }
   return ERROR_NOERROR;
   break;
   case BTREE_LEAF_NODE:
   return ERROR_NOERROR;
   break;
   default:
   if (display_type==BTREE_DEPTH_DOT) { 
   } else {
    o << "Unsupported Node Type " << b.info.nodetype ;
  }
  return ERROR_INSANE;
}

return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  //1) Make sure each block is on either the freelist, the super block, or a btree node. And only ONE.
  //2)Btree has no Cycles (walk tree and guarantee proper structure)
  //3)Freelist has no cycles (how to check this?)
  //4)Interior nodes are only pointed to once.
  //5)leaf nodes are pointed to only once
  //6)no pointers point to free list
  //7)Ordered keys
  //8)Nodes all are within 1/3,2/3 length
  //9)Superblocks key count is same as actual number of keys (how does this account for duplicate keys?)

  //TREE WALK
  //During the tree walk: 
  //make a list of all blocks listed as btree nodes. for use in 1
  //Walk each node, and make sure that they are within minLen/maxLen (what is too empty?), also check that keys are in order: 7,8.
  //If any pointers point to visited nodes, throw error, if any pointers point to freelist nodes, throw error. 4, 5, 2, 6.
  

  //3 and 9 are currently unimplemented.
  //Consider incrementing key count as you walk each node, and comparing to superblock to check 9.
  return ERROR_UNIMPL;
}



ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  return os;
}




