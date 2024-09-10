package db.datastructure;

import java.nio.ByteBuffer;

import db.backend.Pager;
import db.data.Row;
import db.data.Table;

public class BTree<K extends Comparable<K>, V> {

    private final int order;
    private final int KEY_SIZE = Integer.BYTES;
    private final int VALUE_SIZE = 255;

    private BTreeNode<K, V> root;
    private Pager pager;

    public BTree(int order, Pager pager) {
        this.order = Math.max(order, 2); // Ensure minimum order of 2
        this.root = new BTreeNode<>(order);
        this.pager = pager;
    }

    // -------------------------------------
    // B-Tree data structures implementation
    // -------------------------------------
    
    public V search(K key) 
    { return searchInNode(root, key); }

    private V searchInNode(BTreeNode<K, V> node, K key) {
        int i = 0;
        while (i < node.getSize() && key.compareTo(node.getKey(i)) > 0) 
        { i++; }

        if (i < node.getSize() && key.compareTo(node.getKey(i)) == 0) 
        { return node.getValue(i); }

        if (node.isLeaf()) 
        { return null; }
        
        return searchInNode(node.getChild(i), key);
    }

    public void insert(K key, V value) {
        BTreeNode<K, V> r = root;

        if (r.isFull()) {
            BTreeNode<K, V> s = new BTreeNode<>(order);
            root = s;

            s.setLeaf(false);
            s.insertChild(0, r);

            splitChild(s, 0);
            insertNonFull(s, key, value);
        } 
        
        else 
        { insertNonFull(r, key, value); }
    }

    private void insertNonFull(BTreeNode<K, V> node, K key, V value) {
        int i = node.getSize() - 1;
        if (node.isLeaf()) 
        { node.insertKey(key, value); } 
        
        else {
            while (i >= 0 && key.compareTo(node.getKey(i)) < 0) 
            { i--; }

            i++;
            BTreeNode<K, V> child = node.getChild(i);

            if (child == null) {
                child = new BTreeNode<>(order);
                node.insertChild(i, child);
            }

            if (child.isFull()) {
                splitChild(node, i);

                if (key.compareTo(node.getKey(i)) > 0) 
                { i++; }
            }

            insertNonFull(node.getChild(i), key, value);
        }
    }

    private void splitChild(BTreeNode<K, V> x, int i) {
        BTreeNode<K, V> y = x.getChild(i);
        BTreeNode<K, V> z = new BTreeNode<>(order);

        z.setLeaf(y.isLeaf());
        
        int t = (order - 1) / 2;
        for (int j = 0; j < t; j++) 
        { z.insertKey(y.getKey(j + t + 1), y.getValue(j + t + 1)); }
        
        if (!y.isLeaf()) {
            for (int j = 0; j < t + 1; j++) 
            { z.insertChild(j, y.removeChild(j + t + 1)); }
        }
        
        x.insertChild(i + 1, z);
        x.insertKey(y.getKey(t), y.getValue(t));
        
        for (int j = y.getSize() - 1; j >= t; j--) 
        { y.removeKey(j); }
    }

    public void delete(K key) {
        if (root.getSize() == 0) 
        { return; }

        deleteKey(root, key);
        if (root.getSize() == 0 && !root.isLeaf()) 
        { root = root.getChild(0); }
    }

    private void deleteKey(BTreeNode<K, V> x, K key) {
        int i = 0;
        while (i < x.getSize() && key.compareTo(x.getKey(i)) > 0) 
        { i++; }

        if (i < x.getSize() && key.compareTo(x.getKey(i)) == 0) {
            if (x.isLeaf()) 
            { x.removeKey(i); } 
            
            else 
            { deleteInternalNode(x, key, i); }
        } 
        
        else if (!x.isLeaf()) {
            deleteKey(x.getChild(i), key);
            balanceAfterDelete(x, i);
        }
    }

    private void deleteInternalNode(BTreeNode<K, V> x, K key, int i) {
        BTreeNode<K, V> y = x.getChild(i);
        BTreeNode<K, V> z = x.getChild(i + 1);

        if (y.getSize() >= order / 2) {
            K predKey = getPredecessor(y);
            V predValue = search(predKey);

            x.removeKey(i);
            x.insertKey(predKey, predValue);
            deleteKey(y, predKey);
        } 
        
        else if (z.getSize() >= order / 2) {
            K succKey = getSuccessor(z);
            V succValue = search(succKey);
            x.removeKey(i);
            x.insertKey(succKey, succValue);
            deleteKey(z, succKey);
        } 
        
        else {
            mergeNodes(x, i);
            deleteKey(y, key);
        }
    }

    private K getPredecessor(BTreeNode<K, V> node) {
        while (!node.isLeaf()) 
        { node = node.getChild(node.getSize()); }

        return node.getKey(node.getSize() - 1);
    }

    private K getSuccessor(BTreeNode<K, V> node) {
        while (!node.isLeaf()) 
        { node = node.getChild(0); }

        return node.getKey(0);
    }

    private void mergeNodes(BTreeNode<K, V> parent, int index) {
        BTreeNode<K, V> child = parent.getChild(index);
        BTreeNode<K, V> sibling = parent.getChild(index + 1);
        
        child.insertKey(parent.getKey(index), parent.getValue(index));
        
        for (int i = 0; i < sibling.getSize(); i++) 
        { child.insertKey(sibling.getKey(i), sibling.getValue(i)); }
        
        if (!child.isLeaf()) {
            for (int i = 0; i <= sibling.getSize(); i++) 
            { child.insertChild(child.getSize(), sibling.getChild(i)); }
        }
        
        parent.removeKey(index);
        parent.removeChild(index + 1);
    }

    private void balanceAfterDelete(BTreeNode<K, V> x, int i) {
        BTreeNode<K, V> child = x.getChild(i);
        
        if (child.getSize() < order / 2 - 1) {
            BTreeNode<K, V> leftSibling = (i > 0) ? x.getChild(i - 1) : null;
            BTreeNode<K, V> rightSibling = (i < x.getSize()) ? x.getChild(i + 1) : null;

            if (leftSibling != null && leftSibling.getSize() >= order / 2) {
                // Borrow from left sibling
                child.insertKey(x.getKey(i - 1), x.getValue(i - 1));
                x.removeKey(i - 1);
                x.insertKey(leftSibling.getKey(leftSibling.getSize() - 1), 
                            leftSibling.getValue(leftSibling.getSize() - 1));


                if (!child.isLeaf()) 
                { child.insertChild(0, leftSibling.removeChild(leftSibling.getSize())); }

                leftSibling.removeKey(leftSibling.getSize() - 1);
            } 
            
            else if (rightSibling != null && rightSibling.getSize() >= order / 2) {
                // Borrow from right sibling
                child.insertKey(x.getKey(i), x.getValue(i));
                x.removeKey(i);
                x.insertKey(rightSibling.getKey(0), rightSibling.getValue(0));

                if (!child.isLeaf()) 
                { child.insertChild(child.getSize(), rightSibling.removeChild(0)); }
                rightSibling.removeKey(0);
            } 
            
            else {
                // Merge with a sibling
                if (leftSibling != null) 
                { mergeNodes(x, i - 1); }

                else 
                { mergeNodes(x, i); }
            }
        }
    }

    public K getMinKey() {
        if (root == null) 
        { return null; }

        return getMinKey(root);
    }

    private K getMinKey(BTreeNode<K, V> node) {
        if (node.isLeaf()) 
        { return node.getKey(0); }

        return getMinKey(node.getChild(0));
    }

    public K getMaxKey() {
        if (root == null) 
        { return null; }

        return getMaxKey(root);
    }

    private K getMaxKey(BTreeNode<K, V> node) {
        if (node.isLeaf()) 
        { return node.getKey(node.getSize() - 1); }
        
        return getMaxKey(node.getChild(node.getSize()));
    }

    public K getNextKey(K key) {
        if (root == null) {
            return null;
        }
        return getNextKeyInNode(root, key);
    }

    private K getNextKeyInNode(BTreeNode<K, V> node, K key) {
        int i = 0;

        // Find the first key greater than the given key
        while (i < node.getSize() && key.compareTo(node.getKey(i)) >= 0) 
        { i++; }

        if (i < node.getSize()) {
            // If we found a greater key in this node, return it
            return node.getKey(i);
        } 
        
        else if (node.isLeaf()) {
            // If this is a leaf and we didn't find a greater key, there is no next key
            return null;
        } 
        
        else {
            // If this is not a leaf, search in the appropriate child
            BTreeNode<K, V> childNode = node.getChild(i);
            K nextKey = getNextKeyInNode(childNode, key);
            
            if (nextKey != null) 
            { return nextKey; }
            
            // If we didn't find a next key in the child, move to the next child
            if (i < node.getSize()) 
            { return node.getKey(i); }
            
            // If there are no more children, there is no next key
            return null;
        }
    }

    // ------------------------
    // Writing B-Tree into Disk 
    // ------------------------

    public void saveToDisk() 
        throws Exception 
    {
        if (root == null) { 
            // Handle empty tree case
            ByteBuffer metaPage = pager.getPage(0);
            metaPage.putInt(0, -1); 
            metaPage.putInt(4, order);

            pager.flush(0, 8);
        }
        
        else {
            // Start with the root node
            int rootPageNum = saveNodeToDisk(root, -1);
                    
            // Save the root page number and order to a known location (e.g., page 0)
            ByteBuffer metaPage = pager.getPage(0);

            metaPage.putInt(0, rootPageNum);
            metaPage.putInt(4, order);
            pager.flush(0, 8);
        }
        
        pager.getFile().getFD().sync();
    }

    private int saveNodeToDisk(BTreeNode<K, V> node, int parentPageNum) 
        throws Exception 
    {
        if (node == null)
        { return -1; }      // indicates null node

        // Serialize the node
        ByteBuffer serializedNode = serializeNode(node);
        int pageNum = findAvailablePage();
        
        // Write the serialized node to the page
        ByteBuffer page = pager.getPage(pageNum);
        page.put(serializedNode);
        pager.flush(pageNum, serializedNode.limit());
        
        // Update the parent's child pointer (if not root)
        if (parentPageNum != -1) {
            updateParentChildPointer(parentPageNum, node.getKey(0), pageNum);
        }
        
        // Recursively save child nodes
        if (!node.isLeaf()) {
            for (int i = 0; i <= node.getSize(); i++) {
                int childPageNum = saveNodeToDisk(node.getChild(i), pageNum);
                updateChildPointer(pageNum, i, childPageNum);
            }
        }
        
        return pageNum;
    }

    private ByteBuffer serializeNode(BTreeNode<K, V> node) {
        ByteBuffer buffer = ByteBuffer.allocate(Table.PAGE_SIZE);
        buffer.putInt(node.getSize());
        buffer.put((byte) (node.isLeaf() ? 1 : 0));
        
        for (int i = 0; i < node.getSize(); i++) {
            K key = node.getKey(i);
            V value = node.getValue(i);

            serializeKeyValue(buffer, key, value);
        }

        if (!node.isLeaf()) {
           for (int i = 0; i <= node.getSize(); i++) {
                buffer.putInt(node.getChild(i) != null ? i : -1);
            }
        }

        buffer.flip();
        return buffer;
    }

    private void serializeKeyValue(ByteBuffer buffer, K key, V value) {
        buffer.putInt((Integer) key);
        ((Row) value).serializeRow(buffer);
    }

    private void updateParentChildPointer(int parentPageNum, K key, int childPageNum) 
        throws Exception 
    {
        ByteBuffer parentPage = pager.getPage(parentPageNum);
        BTreeNode<K, V> parentNode = deserializeNode(parentPage);
        int index = findChildIndex(parentNode, key);

        updateChildPointer(parentPageNum, index, childPageNum);
    }

    private void updateChildPointer(int pageNum, int childIndex, int childPageNum) 
        throws Exception 
    {
        ByteBuffer page = pager.getPage(pageNum);
        int size = page.getInt(0);

        int childrenOffset = 5 + (size * (KEY_SIZE + VALUE_SIZE));
        int childPointerOffset = childrenOffset + (childIndex * 4);

        page.putInt(childPointerOffset, childPageNum);
        pager.flush(pageNum, Table.PAGE_SIZE);
    }

    @SuppressWarnings("unchecked")
    private BTreeNode<K, V> deserializeNode(ByteBuffer buffer) 
        throws Exception
    {
        int size = buffer.getInt();
        boolean isLeaf = buffer.get() != 0;
        BTreeNode<K, V> node = new BTreeNode<>(this.order);
        node.setLeaf(isLeaf);

        for (int i = 0; i < size; i++) {
            K key = (K) Integer.valueOf(buffer.getInt());
            V value = (V) Row.deserializeRow(buffer);
            node.insertKey(key, value);
        }

        if (!isLeaf) {
            for (int i = 0; i <= size; i++) {
                int childPageNum = buffer.getInt();
                if (childPageNum != -1) {
                    node.insertChild(i, loadNodeFromDisk(childPageNum));
                }
            }
        }

        return node;
    }
    
    private int findChildIndex(BTreeNode<K, V> node, K key) {
        int i = 0;
        while (i < node.getSize() && key.compareTo(node.getKey(i)) > 0) 
        { i++; }

        return i;
    }

    private int findAvailablePage() 
        throws Exception 
    {
        // Implement logic to find an available page
        // This could involve maintaining a free list or simply appending to the end of the file
        // For simplicity, let's just append to the end of the file
        return (int) (pager.getFileLength() / Table.PAGE_SIZE);
    }

    public void loadFromDisk() 
        throws Exception 
    {
        ByteBuffer metaPage = pager.getPage(0);
        int rootPageNum = metaPage.getInt(0);
        int savedOrder = metaPage.getInt(4);

        System.out.println("Saved order: " + savedOrder);
        System.out.println("Current order: " + this.order);
            
        if (savedOrder <= 0) {
            throw new IllegalStateException("Invalid saved B-tree order: " + savedOrder);
        }

        if (savedOrder != this.order) {
            throw new IllegalStateException("Saved B-tree order (" + savedOrder + ") does not match current order (" + this.order + ")");
        }
        
        if (rootPageNum == -1) {
            this.root = new BTreeNode<>(order);
        } else {
            this.root = loadNodeFromDisk(rootPageNum);
        }
    }

    private BTreeNode<K, V> loadNodeFromDisk(int pageNum) 
        throws Exception 
    {
        if (pageNum == -1) {
            return null;
        }

        ByteBuffer page = pager.getPage(pageNum);
        return deserializeNode(page);
    }
}