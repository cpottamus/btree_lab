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

  //Calculate maximum number of keys per bllock
  SIZE_T blockSize = buffercache->GetBlockSize();
  maxNumKeys = blockSize/(16);
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
      rc =  b.SetVal(offset, value);
      if(rc) {return rc;}
      rc = b.Serialize(buffercache, node);
      return rc;
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
  //This is really a B+ tree (with optional sequentially linked leaf nodes).
  //This means that on every key you split, you need to include that key AGAIN in it's >= diskblock, so that is eventually included in a leaf node with it's key/value pair.
  //This is also what makes deleting a key a nightmare, since you have to get rid of ALL instances of the ky (including the leaf version with the value), and then rebalance.
  // page 636 in the book has good diagrams of this
  //cout << "Started insert" << endl;

  //======ALGORITHM======
  VALUE_T val;

  //Lookup and attempt to update key
  ERROR_T retCode = LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, val);

  //cout << "Finished LookupOrUpdateInternal" << endl;

  switch(retCode) {
    //If there is no error in the update call, end function, declare update successful.
    case ERROR_NOERROR:
    //std::cout << "Key already existed, value updated."<<std::endl;
    return ERROR_INSERT;
    //If the key doesn't exist (as expected), begin insert functionality
    case ERROR_NONEXISTENT:
      //traverse to find the leaf
      //Use a stack of pointers to track the path down to the node where the key would go.
    BTreeNode leafNode;
    BTreeNode rootNode;
    BTreeNode rightLeafNode;
    ERROR_T rc;
    SIZE_T leafPtr;
    SIZE_T rightLeafPtr;

    SIZE_T rootPtr = superblock.info.rootnode;
    rootNode.Unserialize(buffercache, rootPtr);
    initBlock = false;
    if (rootNode.info.numkeys != 0) {
      initBlock = true;
    }
    
    rootNode.Serialize(buffercache, rootPtr);

    //If no keys  existent yet...
    if(!initBlock){
      initBlock=true;
      
        //Allocate a new block, and set the values to the first key spot.
      AllocateNode(leafPtr);
      leafNode = BTreeNode(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
      leafNode.Serialize(buffercache, leafPtr);
      rc = leafNode.Unserialize(buffercache, leafPtr);
      if(rc){ return rc;}

      leafNode.info.numkeys++;
      leafNode.SetKey(0, key);
      leafNode.SetVal(0, value);
      //Re-serialize after the access and write. 
      leafNode.Serialize(buffercache, leafPtr);

      //Connect it to root
      rc = rootNode.Unserialize(buffercache, superblock.info.rootnode);
      if(rc){ return rc;}
      rootNode.info.numkeys++;
      rootNode.SetPtr(0, leafPtr);
      rootNode.SetKey(0, key);

      //Built right node and connect
      AllocateNode(rightLeafPtr);
      rightLeafNode = BTreeNode(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
      rc = rightLeafNode.Serialize(buffercache, rightLeafPtr);
      if(rc){ return rc;}
      rootNode.SetPtr(1, rightLeafPtr);
      rc = rootNode.Serialize(buffercache, superblock.info.rootnode);
      if(rc){ return rc;}
    } else{
      std::vector<SIZE_T> pointerPath;
      pointerPath.push_back(superblock.info.rootnode);
      //cout << "Got to LookupLeaf" << endl;
      LookupLeaf(superblock.info.rootnode, key, pointerPath);
      //cout << "Finished LookupLeaf" << endl;
    //Get the node from the last pointer (which points to the leaf node that the key belongs on)
      leafPtr = pointerPath.back();
      // std::cout<<"WE BUILT THIS CITY ON ROCK AND ROLL SHOEFLY Dont bother me ::: "<<leafPtr<<std::endl;
      // std::cout<<"ALSO THIS ::: "<<pointerPath.size()<<std::endl;
      // for(int i =pointerPath.size()-1; i>=0; i--) {
      //   std::cout<<"Little sumpin"<<pointerPath.at(i)<<std::endl;
      // }
      pointerPath.pop_back();
      //cout << "LeafPtr:" << leafPtr << endl;
      KEY_T testkey;
      KEY_T keySpot;
      VALUE_T valSpot;
      rc = leafNode.Unserialize(buffercache, leafPtr);
      //cout << "Unserialized LeafPtr" << endl;
      //Walk the leaf node
      //Increment the key count for the given node.
      leafNode.info.numkeys++;
        //cout << leafNode.info.numkeys << endl;
      if (leafNode.info.numkeys == 1) {
        rc = leafNode.SetKey(0, key);
        if (rc) { return rc;}
        rc = leafNode.SetVal(0, value);
        if (rc) { return rc;}
      } else {
        bool inserted = false;
        for(SIZE_T offset =0; offset<(int)leafNode.info.numkeys-1; offset++){
          rc = leafNode.GetKey(offset, testkey);
          if (rc) { return rc;}
          if(key < testkey || key == testkey) {
        //Once you've found the spot the key needs to go, move all other keys over by 1
            for(int offset2 = (int)leafNode.info.numkeys-2; offset2 >= (int)offset; offset2--){
          //Grab the old key and value
              rc = leafNode.GetKey((SIZE_T)offset2, keySpot);
              if (rc) { return rc;}
              rc = leafNode.GetVal((SIZE_T)offset2, valSpot);
              if (rc) { return rc;}
         //move it up by 1
              rc = leafNode.SetKey((SIZE_T)offset2+1, keySpot);
              if (rc) { return rc;}
              rc = leafNode.SetVal((SIZE_T)offset2+1, valSpot);
              if (rc) { return rc;}
            }

        //assign the new key to offset
            inserted = true;
            rc = leafNode.SetKey(offset, key);
            if (rc) { return rc;}
            rc = leafNode.SetVal(offset, value);
            if (rc) { return rc;}

            break;
          }
        }
        if (!inserted) {
          rc = leafNode.SetKey(leafNode.info.numkeys - 1, key);
          if (rc) { return rc;}
          rc = leafNode.SetVal(leafNode.info.numkeys - 1, value);
          if (rc) { return rc;}
        }
      }
     //Re-serialize after the access and write. 
      leafNode.Serialize(buffercache, leafPtr); 
    //check if the node length is over 2/3, and call rebalance if necessary
      if((int)leafNode.info.numkeys > (int)(2*maxNumKeys/3)) {
          SIZE_T parentPtr = pointerPath.back();
          pointerPath.pop_back();
        rc = Rebalance(parentPtr, pointerPath);
      }
    }

    /*
    default:
    std::cout << "Unexpected Error on look up. Please perform sanityCheck and diagnose issue."<<std::endl;
    return ERROR_INSANE;
    */ 
  }

  //If it exists, call update on it with the new value


  return ERROR_NOERROR;
}

//This lookup function will find the path to the node where the passed in key would go, and return it as a stack of pointers.
ERROR_T BTreeIndex::LookupLeaf(const SIZE_T &node, const KEY_T &key, std::vector<SIZE_T> &pointerPath){
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc = b.Unserialize(buffercache, node);

  if(rc!=ERROR_NOERROR){
    return rc;
  }

  switch(b.info.nodetype){
    case BTREE_ROOT_NODE:
    case BTREE_INTERIOR_NODE:
      // Scan through key/ptr pairs
      //and recurse if possible
    for(offset=0;offset<b.info.numkeys; offset++){
      rc=b.GetKey(offset,testkey);
      if(rc) { return rc; }
      if(key < testkey){
            // OK, so we now have the first key that's larger
            // so we ned to recurse on the ptr immediately previous to 
            // this one, if it exists
        rc=b.GetPtr(offset,ptr);
        if (rc) { return rc; }
          //If there is no error on finding the appropriate pointer, push it onto our stack. 
        pointerPath.push_back(ptr);
        //cout << "PointerPath has: " << pointerPath[0] << endl;
        return LookupLeaf(ptr, key, pointerPath);
      }
    }

      //if we get here, we need to go to the next pointer, if it exists.
    if(b.info.numkeys>0){
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
        //If there is no error on finding the appropriate pointer, push it onto our stack. 
      pointerPath.push_back(ptr);
      return LookupLeaf(ptr, key, pointerPath);
    } else {
        // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
    case BTREE_LEAF_NODE:
    //pointerPath.push_back(node);
    return ERROR_NOERROR;
    break;
    default:
        // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }

  return ERROR_INSANE;

}

//Rebalance takes a path of pointers and a node at the bottom of that path. It will split the node and recursively walk up the parent path
// guaranteeing the sanity of each parent.
ERROR_T BTreeIndex::Rebalance(const SIZE_T &node, std::vector<SIZE_T> ptrPath)
{
  BTreeNode b;
  BTreeNode leftNode;
  BTreeNode rightNode;
  ERROR_T rc;
  SIZE_T offset;
    
  int newType;
  //SIZE_T ptr;
  rc = b.Unserialize(buffercache, node);
  if (rc) { return rc;}
  //std::cout<<":::: Allocating new Nodes :::::"<<std::endl;
  //Allocate 2 new nodes, fill them from the place you're splitting
  SIZE_T leftPtr;
  SIZE_T rightPtr;
  AllocateNode(leftPtr);
  if(b.info.nodetype == BTREE_LEAF_NODE){
    newType = BTREE_LEAF_NODE;
  }else{
    newType = BTREE_INTERIOR_NODE;
  }
  leftNode = BTreeNode(newType, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
  rc = leftNode.Serialize(buffercache, leftPtr);
  AllocateNode(rightPtr);
  rightNode = BTreeNode(newType, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
  rc = rightNode.Serialize(buffercache, rightPtr);
  //Unserialize to write to new nodes
  rc = leftNode.Unserialize(buffercache, leftPtr);
  if (rc) { return rc;}
  rc = rightNode.Unserialize(buffercache, rightPtr);
  if (rc) { return rc;}

  //Variables to hold spot/key vals  
  KEY_T keySpot;
  KEY_T testKey;
  VALUE_T valSpot;
  SIZE_T ptrSpot;

  //Find splitting point
  int midpoint = (b.info.numkeys+0.5)/2;

  //If A leafNode
  if(b.info.nodetype==BTREE_LEAF_NODE){
  //Build left leaf node, include the splitting key (this is a <= B+ tree)
    for(offset = 0; (int)offset < midpoint; offset++){
      //std::cout<<":::: OFFSET for building new left leaf node = "<<offset<<std::endl;
      leftNode.info.numkeys++;

    //Get old node values
      rc = b.GetKey(offset, keySpot);
      if (rc) { return rc;}
      rc = b.GetVal(offset, valSpot);
      if (rc) { return rc;}
    //set values in new left node.
      rc = leftNode.SetKey(offset, keySpot);
      if (rc) { return rc;}
      rc = leftNode.SetVal(offset, valSpot);
      if (rc) { return rc;}
    }
  //Build right leaf node
    int spot=0;
    for(offset = midpoint; offset<b.info.numkeys; offset++){
      //std::cout<<":::: OFFSET (spot) for building new right leaf node = "<<spot<<std::endl;
      //std::cout<<":::: Total Block OFFSET (offset), while rebuilding right leaf node"<<offset<<std::endl;
    //Get values from old node.
      rightNode.info.numkeys++;
      rc = b.GetKey(offset, keySpot);
      if (rc) { return rc;}
      rc = b.GetVal(offset, valSpot);
      if (rc) { return rc;}
    //set values in new right node.
      rc = rightNode.SetKey(spot, keySpot);
      if (rc) { return rc;}
      rc = rightNode.SetVal(spot, valSpot);
      if (rc) { return rc;}
      spot++;
    }
  } else {//if it's an interior node.
      //Build left interior node
  for(offset = 0; (int)offset < midpoint; offset++){
    //std::cout<<":::: OFFSET for building new left interior node = "<<offset<<std::endl;
    leftNode.info.numkeys++;
        //Get old key and pointers
    rc = b.GetKey(offset, keySpot);
    if (rc) { return rc;}
    rc = b.GetPtr(offset, ptrSpot);
    if (rc) { return rc;}
        //Set new key and Pointer vals
    rc = leftNode.SetKey(offset, keySpot);
    if (rc) { return rc;}
    rc = leftNode.SetPtr(offset, ptrSpot);
  }
      //Build Right interior node
  int spot=0;
  for(offset = midpoint; offset<b.info.numkeys; offset++){
    //std::cout<<":::: OFFSET (spot) for building new right interior node = "<<spot<<std::endl;
    //std::cout<<":::: Total Block OFFSET (offset), while rebuilding right interior node"<<offset<<std::endl;
    rightNode.info.numkeys++;
    //Get values from old node.
    rc = b.GetKey(offset, keySpot);
    if (rc) { return rc;}
    rc = b.GetPtr(offset, ptrSpot);
    if (rc) { return rc;}
    //set values in new right node.
    rc = rightNode.SetKey(spot, keySpot);
    if (rc) { return rc;}
    rc = rightNode.SetPtr(spot, ptrSpot);
    if (rc) { return rc;}
    spot++;
  }
  rc = b.GetPtr(offset, ptrSpot);
  if (rc) { return rc;}
  rc = rightNode.SetPtr(spot, ptrSpot);
  if (rc) { return rc;}
}
  //Serialize the new nodes
rc = leftNode.Serialize(buffercache, leftPtr);
if (rc) { return rc;}
rc = rightNode.Serialize(buffercache, rightPtr);
if (rc) { return rc;}
rc = b.Serialize(buffercache, node);

  //Find split key
KEY_T splitKey;
rc = b.GetKey(midpoint-1, splitKey);
if (rc) { return rc;}


  //If we're all the way up at the root, we need to make a new root.
  //  std::cout << ":::: NODE TYPE = " << b.info.nodetype << std::endl;
    
  //  std::cout<<"current node nodetype :::: "<<b.info.nodetype<<std::endl;
if (b.info.nodetype == BTREE_ROOT_NODE) {
  //std::cout<<":::: AT THE TOP, BUILDING A NEW ROOT ::::"<<std::endl;
  SIZE_T newRootPtr;
  BTreeNode newRootNode;
  AllocateNode(newRootPtr);
  newRootNode = BTreeNode(BTREE_ROOT_NODE, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
  superblock.info.rootnode = newRootPtr;
    newRootNode.info.rootnode = newRootPtr;
    newRootNode.info.numkeys = 1;
    newRootNode.SetKey(0, splitKey);
    newRootNode.SetPtr(0, leftPtr);
    newRootNode.SetPtr(1, rightPtr);
  rc = newRootNode.Serialize(buffercache, newRootPtr);
  if(rc) {return rc;}
//std::cout<<"::: We made it here! Root node"<<std::endl;
}
else{
//Find the parent node
  SIZE_T parentPtr = ptrPath.back();
//  std::cout<<"WE BUILT THIS CITY ON ROCK AND ROLL COW ::: "<<parentPtr<<std::endl;
      //std::cout<<"ALSO THIS  IN OUR REBALANCE::: "<<ptrPath.size()<<std::endl;
//    for(int i =ptrPath.size()-1; i>=0; i--) {
//        std::cout<<"Little sumpin"<<ptrPath.at(i)<<std::endl;
//      }
  ptrPath.pop_back();
  BTreeNode parentNode;
  rc = parentNode.Unserialize(buffercache, parentPtr);
  if(rc) {return rc;}

    if (parentNode.info.nodetype == BTREE_SUPERBLOCK) {
        AllocateNode(parentPtr);
        
    }
//Increment the key count for the given node.
  //parentNode.info.numkeys++;
    
    BTreeNode newParentNode = BTreeNode(parentNode.info.nodetype, superblock.info.keysize, superblock.info.valuesize, superblock.info.blocksize);
    newParentNode.info.numkeys = parentNode.info.numkeys + 1;
    
    bool newKeyInserted = false;
    for (offset = 0; offset < newParentNode.info.numkeys - 1; offset++) {
        rc = parentNode.GetKey(offset, testKey);
        //    if(rc){ return rc;}
        if (newKeyInserted) {
            rc = parentNode.GetKey(offset, keySpot);
            newParentNode.SetKey(offset + 1, keySpot);
            
            rc = parentNode.GetPtr(offset + 1, ptrSpot);
            newParentNode.SetPtr(offset + 2, ptrSpot);
        } else {
            if (splitKey < testKey) {
                newKeyInserted = true;
                newParentNode.SetPtr(offset, leftPtr);
                newParentNode.SetKey(offset, splitKey);
                newParentNode.SetPtr(offset+1, rightPtr);
                offset = offset - 1;
                
            } else {
                rc = parentNode.GetKey(offset, keySpot);
                newParentNode.SetKey(offset, keySpot);
                
                rc = parentNode.GetPtr(offset, ptrSpot);
                newParentNode.SetPtr(offset, ptrSpot);
            }
        }
    }
    
    newParentNode.Serialize(buffercache, parentPtr);
    
    
//find split keys spot in parent (interior) node, insert it and update keys and pointers.
//  for(offset = 0; offset<parentNode.info.numkeys-1; offset++){
//    //std::cout<<":::: Searching Interior Nodes for splitKey Insertion ::::: offset = "<<offset<<std::endl;
//    rc = parentNode.GetKey(offset, testKey);
//    if(rc){ return rc;}
//    if(splitKey < testKey || splitKey == testKey){ // if testkey > splitkey
//      //std::cout<<":::: Moving through the parent node for rebalance insertion ::: Number of keys in parent = "<<parentNode.info.numkeys<<std::endl;
//      //std::cout<<":::: Moving through the parent node for rebalance insertion ::: parent nodetype = "<<parentNode.info.nodetype<<std::endl;
//          //Once you've found the insertion point for the new key, move all other keys & pointers over by 1
//      for(offset2= parentNode.info.numkeys-2 ; offset2 >= offset; offset2-- ){
//        //std::cout<<":::: Found INSERTION POINT, moving spots over :::: = offset"<<offset<<std::endl;
//            //Grab the old key and pointer
//        rc = parentNode.GetKey(offset2, keySpot);
//        if(rc){ return rc;}
//        rc = parentNode.GetPtr(offset2, ptrSpot);
//        if(rc){ return rc;}
//            //Move it up by 1
//        rc = parentNode.SetKey(offset2+1, keySpot);
//        if(rc){ return rc;}
//        rc = parentNode.SetPtr(offset2+2, ptrSpot);
//      }
//          //We now have moved every pointer over except for the  1 to the immediate right of where we will be inserting our splitKey
//          //Set our pointers and our new key
//      rc = parentNode.SetPtr(offset2+1, rightPtr);
//      if(rc){ return rc;}
//      rc = parentNode.SetPtr(offset,leftPtr);
//      if(rc){ return rc;}
//      rc = parentNode.SetKey(offset, splitKey);
//      if(rc){ return rc;}
//
//      break;
//    }
//  }


  //Check the length of the node and call rebalance if necessary
  //parentNode.Serialize(buffercache, parentPtr);

  if((int)newParentNode.info.numkeys > (int)(2*maxNumKeys/3)){
    rc = Rebalance(parentPtr, ptrPath);
    if(rc){ return rc;}
  }
}
  //Deallocate the old (too large) node
DeallocateNode(node);
return ERROR_NOERROR;
}

ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{

  VALUE_T val = value;
  // WRITE ME
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, val);
  return ERROR_NOERROR;
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
  //7)Ordered keys
  //9)Superblocks key count is same as actual number of keys (how does this account for duplicate keys?)

  //DEFINE FOR SIZE_T and use pointers instead of nodes for use of comparison operators.
  //std::set<BTreeNode> allTreeNodes;

  //Call Sanity Walk on top of tree using superblock.info.rootnode, etc...
  ERROR_T retCode = SanityWalk(superblock.info.rootnode/*, allTreeNodes*/);

  //TODO :: Check all of freelist to see if there are any duplicate components


return retCode;
  

}

//We'll use this for walking the tree for our sanity check.
ERROR_T BTreeIndex::SanityWalk(const SIZE_T &node/*, std::set<BTreeNode> &allTreeNodes*/) const{
BTreeNode b;
ERROR_T rc;
SIZE_T offset;
KEY_T testkey;
KEY_T tempkey;
SIZE_T ptr;
VALUE_T value;

rc = b.Unserialize(buffercache, node);

  //Check if node is already in our BTree
  // bool is_in = allTreeNodes.find(b) != allTreeNodes.end();
  // if(is_in) {
  //   std::cout<<"node "<<b<<" has already been visited by this BTree"<<std::endl;
  // }
  // allTreeNodes.insert(b);

if(rc!=ERROR_NOERROR){
  return rc;
}

      //Check to see if the nodes have proper lengths
if(b.info.numkeys>(int)(2*maxNumKeys/3)){
  std::cout << "Current Node of type "<<b.info.nodetype<<" has "<<b.info.numkeys<<" keys. Which is over the 2/3 threshold of the maximum of "<<maxNumKeys<<" keys."<<std::endl;
}

switch(b.info.nodetype){
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
      //Scan through key/ptr pairs
      //and recurse if possible

    //TODO :: Push node onto set, where we can check against other visited nodes.  4, 5.

  for(offset=0; offset<b.info.numkeys; offset++){
    rc = b.GetKey(offset,testkey);
    if(rc) {return rc; }

      //If keys are not in proper size order
    if(offset+1<b.info.numkeys-1){
      rc = b.GetKey(offset+1, tempkey);
      if(tempkey < testkey){
        std::cout<<"The keys are not properly sorted!"<<std::endl;
      }

    }

    rc=b.GetPtr(offset,ptr);
    if(rc){return rc;}

        return SanityWalk(ptr/*, allTreeNodes*/);

//      if(key<testkey){
            // OK, so we now have the first key that's larger
            // so we ned to recurse on the ptr immediately previous to 
            // this one, if it exists
        // rc=b.GetPtr(offset,ptr);
        // if(rc){return rc;}

        // return SanityWalk(ptr, key);
   //   }
  }

    //If we get here, we need to go to the next pointer, if it exists.
  if(b.info.numkeys>0){
    rc = b.GetPtr(b.info.numkeys, ptr);
    if(rc) { return rc; }

      return SanityWalk(ptr/*, allTreeNodes*/);
  }else{
      //There are no keys at all on this node, so nowhere to go
    std::cout << "The keys on this interior node are nonexistent."<<std::endl;
    return ERROR_NONEXISTENT;
  }
  break;
  case BTREE_LEAF_NODE:

  for(offset=0; offset<b.info.numkeys;offset++){
    rc = b.GetKey(offset, testkey);
    if(rc) { 
      std::cout << "Leaf Node is missing key"<<std::endl;
      return rc;
    }
    rc =b.GetVal(offset, value);
    if(rc){
      std::cout << "leaf node key is missing associated value"<<std::endl;
      return rc;
    }

      //If keys are not in proper size order
    if(offset+1<b.info.numkeys){
      rc = b.GetKey(offset+1, tempkey);
      if(tempkey < testkey){
        std::cout<<"The keys are not properly sorted!"<<std::endl;
      }
    }
  }
  break;
  default:

  return ERROR_INSANE;
  break;
}

return ERROR_NOERROR;
}



ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  ERROR_T rc;
  rc = Display(os, BTREE_DEPTH_DOT);
  return os;
}




