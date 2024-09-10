package db.datastructure;

import db.backend.Pager;

import java.nio.ByteBuffer;

public class BTree<K extends Comparable<K>, V> {

    private final int order;
    private final Pager pager;
    private long rootPageNum;
    private int numNodes;

    // constructor for a new tree
    public BTree(int order, Pager pager) 
        throws Exception 
    {
        this.order = order;
        this.pager = pager;
        this.numNodes = 1;  

        this.rootPageNum = allocateNewPage();
        BTreeNode<K, V> root = new BTreeNode<>(order, rootPageNum);
        writeNode(root);
    }

    // constructor for reconstructing  the tree form disk
    public BTree(int order, Pager pager, long rootPageNum, int numNodes) {
        this.order = order;
        this.pager = pager;
        this.rootPageNum = rootPageNum;
        this.numNodes = numNodes;
    }


    private long allocateNewPage() throws Exception {
        return pager.getFreePage();
    }

    private void writeNode(BTreeNode<K, V> node) throws Exception {
        ByteBuffer page = pager.getPage((int) node.getPageNumber());
        page.put(node.serialize());
        pager.writePage((int) node.getPageNumber());
    }

    private BTreeNode<K, V> readNode(long pageNum) throws Exception {
        ByteBuffer page = pager.getPage((int) pageNum);
        byte[] data = new byte[page.capacity()];
        page.get(data);
        return BTreeNode.deserialize(data);
    }

    public V search(K key) throws Exception {
        return searchInNode(rootPageNum, key);
    }

    private V searchInNode(long nodePageNum, K key) throws Exception {
        BTreeNode<K, V> node = readNode(nodePageNum);
        int i = 0;
        while (i < node.getSize() && key.compareTo(node.getKey(i)) > 0) {
            i++;
        }

        if (i < node.getSize() && key.compareTo(node.getKey(i)) == 0) {
            return node.getValue(i);
        }

        if (node.isLeaf()) {
            return null;
        }
        
        return searchInNode(node.getChildPageNumber(i), key);
    }

    public void insert(K key, V value) throws Exception {
        BTreeNode<K, V> r = readNode(rootPageNum);

        if (r.isFull()) {
            long newRootPageNum = allocateNewPage();
            BTreeNode<K, V> s = new BTreeNode<>(order, newRootPageNum);
            rootPageNum = newRootPageNum;

            s.setLeaf(false);
            s.insertChild(0, r.getPageNumber());

            splitChild(s, 0);
            insertNonFull(s, key, value);
            writeNode(s);
        } else {
            insertNonFull(r, key, value);
        }

        numNodes += 1;
    }

    private void insertNonFull(BTreeNode<K, V> node, K key, V value) throws Exception {
        int i = node.getSize() - 1;
        if (node.isLeaf()) {
            node.insertKey(key, value);
            writeNode(node);
        } else {
            while (i >= 0 && key.compareTo(node.getKey(i)) < 0) {
                i--;
            }
            i++;
            BTreeNode<K, V> child = readNode(node.getChildPageNumber(i));

            if (child.isFull()) {
                splitChild(node, i);
                if (key.compareTo(node.getKey(i)) > 0) {
                    i++;
                }
                child = readNode(node.getChildPageNumber(i));
            }

            insertNonFull(child, key, value);
        }
    }

    private void splitChild(BTreeNode<K, V> x, int i) throws Exception {
        long newChildPageNum = allocateNewPage();
        BTreeNode<K, V> z = new BTreeNode<>(order, newChildPageNum);
        BTreeNode<K, V> y = readNode(x.getChildPageNumber(i));
        z.setLeaf(y.isLeaf());
        
        int t = (order - 1) / 2;
        for (int j = 0; j < t; j++) {
            z.insertKey(y.getKey(j + t + 1), y.getValue(j + t + 1));
        }
        
        if (!y.isLeaf()) {
            for (int j = 0; j < t + 1; j++) {
                z.insertChild(j, y.getChildPageNumber(j + t + 1));
            }
            for (int j = y.getSize(); j >= t + 1; j--) {
                y.removeChild(j);
            }
        }
        
        x.insertChild(i + 1, z.getPageNumber());
        x.insertKey(y.getKey(t), y.getValue(t));
        
        for (int j = y.getSize() - 1; j >= t; j--) {
            y.removeKey(j);
        }

        writeNode(x);
        writeNode(y);
        writeNode(z);
    }

    public void delete(K key) throws Exception {
        BTreeNode<K, V> root = readNode(rootPageNum);
        if (root.getSize() == 0) {
            return;
        }

        deleteKey(root, key);
        if (root.getSize() == 0 && !root.isLeaf()) {
            rootPageNum = root.getChildPageNumber(0);
        }
        writeNode(root);
        numNodes--;  
    }

    private void deleteKey(BTreeNode<K, V> x, K key) throws Exception {
        int i = 0;
        while (i < x.getSize() && key.compareTo(x.getKey(i)) > 0) {
            i++;
        }

        if (i < x.getSize() && key.compareTo(x.getKey(i)) == 0) {
            if (x.isLeaf()) {
                x.removeKey(i);
                writeNode(x);
            } else {
                deleteInternalNode(x, key, i);
            }
        } else if (!x.isLeaf()) {
            BTreeNode<K, V> child = readNode(x.getChildPageNumber(i));
            deleteKey(child, key);
            balanceAfterDelete(x, i);
            writeNode(x);
        }
    }

    private void deleteInternalNode(BTreeNode<K, V> x, K key, int i) throws Exception {
        BTreeNode<K, V> y = readNode(x.getChildPageNumber(i));
        BTreeNode<K, V> z = readNode(x.getChildPageNumber(i + 1));

        if (y.getSize() >= order / 2) {
            K predKey = getPredecessor(y);
            V predValue = search(predKey);
            x.removeKey(i);
            x.insertKey(predKey, predValue);
            deleteKey(y, predKey);
        } else if (z.getSize() >= order / 2) {
            K succKey = getSuccessor(z);
            V succValue = search(succKey);
            x.removeKey(i);
            x.insertKey(succKey, succValue);
            deleteKey(z, succKey);
        } else {
            mergeNodes(x, i);
            deleteKey(y, key);
        }
        writeNode(x);
        writeNode(y);
        writeNode(z);
    }

    private K getPredecessor(BTreeNode<K, V> node) throws Exception {
        while (!node.isLeaf()) {
            node = readNode(node.getChildPageNumber(node.getSize()));
        }
        return node.getKey(node.getSize() - 1);
    }

    private K getSuccessor(BTreeNode<K, V> node) throws Exception {
        while (!node.isLeaf()) {
            node = readNode(node.getChildPageNumber(0));
        }
        return node.getKey(0);
    }

    private void mergeNodes(BTreeNode<K, V> parent, int index) throws Exception {
        BTreeNode<K, V> child = readNode(parent.getChildPageNumber(index));
        BTreeNode<K, V> sibling = readNode(parent.getChildPageNumber(index + 1));
        
        child.insertKey(parent.getKey(index), parent.getValue(index));
        
        for (int i = 0; i < sibling.getSize(); i++) {
            child.insertKey(sibling.getKey(i), sibling.getValue(i));
        }
        
        if (!child.isLeaf()) {
            for (int i = 0; i <= sibling.getSize(); i++) {
                child.insertChild(child.getSize(), sibling.getChildPageNumber(i));
            }
        }
        
        parent.removeKey(index);
        parent.removeChild(index + 1);

        writeNode(parent);
        writeNode(child);

        // Free the page used by the sibling
        pager.freePage((int) sibling.getPageNumber());
    }

    private void balanceAfterDelete(BTreeNode<K, V> x, int i) throws Exception {
        BTreeNode<K, V> child = readNode(x.getChildPageNumber(i));
        
        if (child.getSize() < order / 2 - 1) {
            BTreeNode<K, V> leftSibling = (i > 0) ? readNode(x.getChildPageNumber(i - 1)) : null;
            BTreeNode<K, V> rightSibling = (i < x.getSize()) ? readNode(x.getChildPageNumber(i + 1)) : null;

            if (leftSibling != null && leftSibling.getSize() >= order / 2) {
                // Borrow from left sibling
                child.insertKey(x.getKey(i - 1), x.getValue(i - 1));
                x.removeKey(i - 1);
                x.insertKey(leftSibling.getKey(leftSibling.getSize() - 1), 
                            leftSibling.getValue(leftSibling.getSize() - 1));

                if (!child.isLeaf()) {
                    child.insertChild(0, leftSibling.getChildPageNumber(leftSibling.getSize()));
                    leftSibling.removeChild(leftSibling.getSize());
                }

                leftSibling.removeKey(leftSibling.getSize() - 1);

                writeNode(x);
                writeNode(child);
                writeNode(leftSibling);
            } else if (rightSibling != null && rightSibling.getSize() >= order / 2) {
                // Borrow from right sibling
                child.insertKey(x.getKey(i), x.getValue(i));
                x.removeKey(i);
                x.insertKey(rightSibling.getKey(0), rightSibling.getValue(0));

                if (!child.isLeaf()) {
                    child.insertChild(child.getSize(), rightSibling.getChildPageNumber(0));
                    rightSibling.removeChild(0);
                }
                rightSibling.removeKey(0);

                writeNode(x);
                writeNode(child);
                writeNode(rightSibling);
            } else {
                // Merge with a sibling
                if (leftSibling != null) {
                    mergeNodes(x, i - 1);
                } else {
                    mergeNodes(x, i);
                }
            }
        }
    }

    public K getMinKey() throws Exception {
        if (rootPageNum == 0) {
            return null;
        }
        return getMinKey(rootPageNum);
    }

    private K getMinKey(long nodePageNum) throws Exception {
        BTreeNode<K, V> node = readNode(nodePageNum);
        if (node.isLeaf()) {
            return node.getKey(0);
        }
        return getMinKey(node.getChildPageNumber(0));
    }

    public long getRootPageNum() 
    { return rootPageNum; }

    public int getNumNodes() 
    { return numNodes; }

    public K getMaxKey() throws Exception {
        if (rootPageNum == 0) 
        { return null; }

        return getMaxKey(rootPageNum);
    }

    private K getMaxKey(Long nodePageNum) throws Exception {
        BTreeNode<K, V> node = readNode(nodePageNum);

        if (node.isLeaf()) {
            return node.getKey(node.getSize() - 1);
        }

        return getMaxKey(node.getChildPageNumber(node.getSize()));
    }

    public K getNextKey(K key) 
        throws Exception
    {
        if (rootPageNum == 0) {
            return null;
        }
        return getNextKeyInNode(rootPageNum, key);
    }

    private K getNextKeyInNode(Long nodePageNum, K key) 
        throws Exception
    {
        int i = 0;
        BTreeNode<K, V> node = readNode(nodePageNum);

        while (i < node.getSize() && key.compareTo(node.getKey(i)) >= 0) 
        { i++; }

        if (i < node.getSize()) {
            return node.getKey(i);
        } 
        
        else if (node.isLeaf()) {
            return null;
        } 
        
        else {
            Long childNodePageNumber = node.getChildPageNumber(i);
            K nextKey = getNextKeyInNode(childNodePageNumber, key);
            
            if (nextKey != null) 
            { return nextKey; }
            
            if (i < node.getSize()) 
            { return node.getKey(i); }
            
            return null;
        }
    }
}