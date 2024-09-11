package db.datastructure;

import java.util.ArrayList;
import java.util.List;

/*
 * Represents a Node in the BTree DS
 * Use generics to allow different type of key and values
 * The keys must be a Comparable         
 */

public class BTreeNode<K extends Comparable<K>, V> {        

    private final int order;        // max number of a children node can have
    private final List<K> keys;
    private final List<V> values;
    private final List<BTreeNode<K, V>> children;

    private boolean isLeaf;     // leaf is a node without children
    private int size;           // number of cuurently keys in the node

    public BTreeNode(int order) {
        this.order = Math.max(2, order); // Ensure order is at least 2
        this.keys = new ArrayList<>(order - 1);
        this.values = new ArrayList<>(order - 1);
        this.children = new ArrayList<>(order);
        this.isLeaf = true;
        this.size = 0;
    }

    public void insertKey(K key, V value) {
        if (size == 0) {
            keys.add(key);
            values.add(value);
            size++;
            return;
        }

        int i = size - 1;
        while (i >= 0 && keys.get(i).compareTo(key) > 0) {
            if (i + 1 < order - 1) {
                keys.add(i + 1, keys.get(i));
                values.add(i + 1, values.get(i));
            }
            i--;
        }

        if (i + 1 < order - 1) {
            keys.add(i + 1, key);
            values.add(i + 1, value);
            size++;
        }
    }

    public void insertChild(int index, BTreeNode<K, V> child) 
    { children.add(index, child); }

    public K removeKey(int index) {
        size--;
        K key = keys.remove(index);
        values.remove(index);

        return key;
    }

    public BTreeNode<K, V> removeChild(int index) 
    { return children.remove(index); }

    // reset the Node
    public void clear() {
        keys.clear();
        values.clear();
        children.clear();

        size = 0;
        isLeaf = true;
    }

    // check if this Node has reached his max capacity 
    public boolean isFull() 
    { return size == order - 1; }

    public boolean isLeaf() 
    { return isLeaf; }

    public void setLeaf(boolean leaf) 
    { isLeaf = leaf; }

    public int getSize() 
    { return size; }

    public K getKey(int index) 
    { return keys.get(index); }

    public V getValue(int index) 
    { return values.get(index); }

    public BTreeNode<K, V> getChild(int index) 
    { return children.get(index); }
}