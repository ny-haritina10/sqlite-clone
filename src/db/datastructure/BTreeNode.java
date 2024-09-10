package db.datastructure;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import db.data.Row;

/*
 * Represents a Node in the BTree DS
 * Use generics to allow different type of key and values
 * The keys must be a Comparable         
 */

public class BTreeNode<K extends Comparable<K>, V> {        

    private final int order;
    private final List<K> keys;
    private final List<V> values;
    private final List<Long> childrenPageNumbers; // Store page numbers instead of BTreeNode objects

    private long pageNumber; // Add pageNumber field
    private boolean isLeaf;     // leaf is a node without children
    private int size;           // number of cuurently keys in the node

    public BTreeNode(int order, long pageNumber) {
        this.order = order;
        this.keys = new ArrayList<>(order - 1);
        this.values = new ArrayList<>(order - 1);
        this.childrenPageNumbers = new ArrayList<>(order);
        this.isLeaf = true;
        this.size = 0;
        this.pageNumber = pageNumber;
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

    public K removeKey(int index) {
        size--;
        K key = keys.remove(index);
        values.remove(index);

        return key;
    }

    // reset the Node
    public void clear() {
        keys.clear();
        values.clear();
        childrenPageNumbers.clear();

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

    public Long getChild(int index) 
    { return childrenPageNumbers.get(index); }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(calculateSerializedSize());
        
        buffer.putInt(order);
        buffer.putInt(size);
        buffer.put((byte) (isLeaf ? 1 : 0));
        buffer.putLong(pageNumber);

        for (int i = 0; i < size; i++) {
            serializeKey(buffer, keys.get(i));
            serializeValue(buffer, values.get(i));
        }

        for (int i = 0; i <= size; i++) {
            if (!isLeaf) {
                buffer.putLong(childrenPageNumbers.get(i));
            }
        }

        return buffer.array();
    }

    public static <K extends Comparable<K>, V> BTreeNode<K, V> deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int order = buffer.getInt();
        int size = buffer.getInt();
        boolean isLeaf = buffer.get() == 1;
        long pageNumber = buffer.getLong();

        BTreeNode<K, V> node = new BTreeNode<>(order, pageNumber);
        node.size = size;
        node.isLeaf = isLeaf;

        for (int i = 0; i < size; i++) {
            K key = deserializeKey(buffer);
            V value = deserializeValue(buffer);
            node.keys.add(key);
            node.values.add(value);
        }

        if (!isLeaf) {
            for (int i = 0; i <= size; i++) {
                long childPageNumber = buffer.getLong();
                node.childrenPageNumbers.add(childPageNumber);
            }
        }

        return node;
    }

    private int calculateSerializedSize() {
        int size = Integer.BYTES * 2 + 1 + Long.BYTES; // order + size + isLeaf + pageNumber
        size += (keys.size() * (getKeySize() + getValueSize()));
        if (!isLeaf) {
            size += (childrenPageNumbers.size() * Long.BYTES);
        }
        return size;
    }

    private void serializeKey(ByteBuffer buffer, K key) 
    { buffer.putInt((Integer) key); }

    private void serializeValue(ByteBuffer buffer, V value) {
        buffer.putInt(((Row) value).getId());
        buffer.put(((Row) value).getUsername().getBytes());
        buffer.put(((Row) value).getEmail().getBytes());
    }

    @SuppressWarnings("unchecked")
    private static <K> K deserializeKey(ByteBuffer buffer) {
        return (K) Integer.valueOf(buffer.getInt());
    }

    @SuppressWarnings("unchecked")
    private static <V> V deserializeValue(ByteBuffer buffer) {
        int id = buffer.getInt();
        byte[] usernameBytes = new byte[32]; // Assuming fixed size
        buffer.get(usernameBytes);
        byte[] emailBytes = new byte[255]; // Assuming fixed size
        buffer.get(emailBytes);

        return (V) new Row(id, new String(usernameBytes).trim(), new String(emailBytes).trim());
    }

    private int getKeySize() 
    { return Integer.BYTES; }

    private int getValueSize() 
    { return Integer.BYTES + 32 + 255; } // id + username + email 

    public void insertChild(int index, long childPageNumber) {
        childrenPageNumbers.add(index, childPageNumber);
    }

    public long removeChild(int index) {
        return childrenPageNumbers.remove(index);
    }

    public long getChildPageNumber(int index) {
        return childrenPageNumbers.get(index);
    }

    public long getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(long pageNumber) {
        this.pageNumber = pageNumber;
    }
}