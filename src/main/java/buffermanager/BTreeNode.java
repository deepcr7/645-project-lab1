package buffermanager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.nio.BufferOverflowException;

abstract class BTreeNode<K extends Comparable<K>> {
    protected int pageId;
    protected boolean isLeaf;
    protected List<K> keys;
    protected int parentPageId;

    public BTreeNode(int pageId, boolean isLeaf) {
        this.pageId = pageId;
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.parentPageId = -1;
    }

    public abstract byte[] serialize();

    public abstract void deserialize(byte[] data);

    public int getPageId() {
        return pageId;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public List<K> getKeys() {
        return keys;
    }

    public int getParentPageId() {
        return parentPageId;
    }

    public void setParentPageId(int parentPageId) {
        this.parentPageId = parentPageId;
    }
}

class BTreeInternalNode<K extends Comparable<K>> extends BTreeNode<K> {
    private List<Integer> childrenPageIds;

    public BTreeInternalNode(int pageId) {
        super(pageId, false);
        this.childrenPageIds = new ArrayList<>();
    }

    public List<Integer> getChildrenPageIds() {
        return childrenPageIds;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(PageImpl.PAGE_SIZE);

        try {

            buffer.putInt(pageId);
            buffer.put((byte) (isLeaf ? 1 : 0));
            buffer.putInt(parentPageId);

            int maxKeys = Math.min(keys.size(), 50);
            buffer.putInt(maxKeys);

            for (int i = 0; i < maxKeys; i++) {
                K key = keys.get(i);
                String keyStr = key.toString();
                if (keyStr.length() > 30) {
                    keyStr = keyStr.substring(0, 30);
                }
                byte[] keyBytes = keyStr.getBytes();

                buffer.putInt(keyBytes.length);
                buffer.put(keyBytes);

                buffer.putInt(childrenPageIds.get(i));
            }

            if (!childrenPageIds.isEmpty() && maxKeys > 0) {
                buffer.putInt(childrenPageIds.get(Math.min(maxKeys, childrenPageIds.size() - 1)));
            }
        } catch (BufferOverflowException e) {
            System.err.println("Buffer overflow in internal node serialization, reducing key count");
            return serializeWithFewerKeys();
        }

        return buffer.array();
    }

    private byte[] serializeWithFewerKeys() {
        ByteBuffer buffer = ByteBuffer.allocate(PageImpl.PAGE_SIZE);

        buffer.putInt(pageId);
        buffer.put((byte) (isLeaf ? 1 : 0));
        buffer.putInt(parentPageId);

        int maxKeys = Math.min(keys.size(), 10);
        buffer.putInt(maxKeys);

        for (int i = 0; i < maxKeys; i++) {
            K key = keys.get(i);
            String keyStr = key.toString();
            if (keyStr.length() > 20) {
                keyStr = keyStr.substring(0, 20);
            }
            byte[] keyBytes = keyStr.getBytes();

            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);

            buffer.putInt(childrenPageIds.get(i));
        }

        if (!childrenPageIds.isEmpty() && maxKeys > 0) {
            buffer.putInt(childrenPageIds.get(Math.min(maxKeys, childrenPageIds.size() - 1)));
        }

        return buffer.array();
    }

    @Override
    public void deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        this.pageId = buffer.getInt();
        this.isLeaf = buffer.get() == 1;
        this.parentPageId = buffer.getInt();
        int keyCount = buffer.getInt();

        keys.clear();
        childrenPageIds.clear();

        for (int i = 0; i < keyCount; i++) {
            int keyLength = buffer.getInt();
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            String keyStr = new String(keyBytes);

            @SuppressWarnings("unchecked")
            K key = (K) keyStr;
            keys.add(key);

            int childPageId = buffer.getInt();
            childrenPageIds.add(childPageId);
        }

        if (keyCount > 0) {
            int childPageId = buffer.getInt();
            childrenPageIds.add(childPageId);
        }
    }
}

class BTreeLeafNode<K extends Comparable<K>> extends BTreeNode<K> {
    private List<List<Rid>> values;
    private int nextLeafPageId;

    public BTreeLeafNode(int pageId) {
        super(pageId, true);
        this.values = new ArrayList<>();
        this.nextLeafPageId = -1;
    }

    public List<List<Rid>> getValues() {
        return values;
    }

    public int getNextLeafPageId() {
        return nextLeafPageId;
    }

    public void setNextLeafPageId(int nextLeafPageId) {
        this.nextLeafPageId = nextLeafPageId;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(PageImpl.PAGE_SIZE);

        try {
            buffer.putInt(pageId);
            buffer.put((byte) (isLeaf ? 1 : 0));
            buffer.putInt(parentPageId);

            int availableSpace = PageImpl.PAGE_SIZE - 20;

            int maxKeysToWrite = Math.min(keys.size(), 50);

            buffer.putInt(maxKeysToWrite);
            buffer.putInt(nextLeafPageId);

            int keysWritten = 0;
            for (int i = 0; i < keys.size() && keysWritten < maxKeysToWrite; i++) {
                K key = keys.get(i);
                String keyStr = key.toString();

                if (keyStr.length() > 100) {
                    keyStr = keyStr.substring(0, 100);
                }
                byte[] keyBytes = keyStr.getBytes();

                List<Rid> ridList = values.get(i);
                int maxRids = Math.min(ridList.size(), 20);
                int spaceNeeded = 4 + keyBytes.length + 4 + (maxRids * 8);

                if (buffer.remaining() < spaceNeeded) {
                    break;
                }
                buffer.putInt(keyBytes.length);
                buffer.put(keyBytes);
                buffer.putInt(maxRids);
                for (int j = 0; j < maxRids; j++) {
                    Rid rid = ridList.get(j);
                    buffer.putInt(rid.getPageId());
                    buffer.putInt(rid.getSlotId());
                }

                keysWritten++;
            }

            if (keysWritten < maxKeysToWrite) {
                buffer.position(9);
                buffer.putInt(keysWritten);
            }

        } catch (Exception e) {
            System.err.println("Error serializing leaf node " + pageId + ": " + e.getMessage());
            buffer.clear();
            buffer.putInt(pageId);
            buffer.put((byte) (isLeaf ? 1 : 0));
            buffer.putInt(parentPageId);
            buffer.putInt(0);
            buffer.putInt(nextLeafPageId);
        }

        return buffer.array();
    }

    @Override
    public void deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        try {
            this.pageId = buffer.getInt();
            this.isLeaf = buffer.get() == 1;
            this.parentPageId = buffer.getInt();
            int keyCount = buffer.getInt();
            this.nextLeafPageId = buffer.getInt();
            if (keyCount < 0 || keyCount > 1000) {
                System.err.println("Warning: Invalid key count " + keyCount + " in node " + pageId);
                keyCount = 0;
            }
            keys.clear();
            values.clear();
            for (int i = 0; i < keyCount && buffer.remaining() >= 8; i++) {
                try {
                    int keyLength = buffer.getInt();
                    if (keyLength <= 0 || keyLength > buffer.remaining()) {
                        System.err.println("Warning: Invalid key length " + keyLength);
                        break;
                    }
                    byte[] keyBytes = new byte[keyLength];
                    buffer.get(keyBytes);
                    String keyStr = new String(keyBytes);

                    @SuppressWarnings("unchecked")
                    K key = (K) keyStr;
                    keys.add(key);
                    if (buffer.remaining() < 4)
                        break;
                    int ridCount = buffer.getInt();
                    if (ridCount < 0 || ridCount > 1000 || buffer.remaining() < ridCount * 8) {
                        System.err.println("Warning: Invalid RID count " + ridCount);
                        break;
                    }
                    List<Rid> ridList = new ArrayList<>();
                    for (int j = 0; j < ridCount; j++) {
                        int ridPageId = buffer.getInt();
                        int ridSlotId = buffer.getInt();
                        ridList.add(new Rid(ridPageId, ridSlotId));
                    }
                    values.add(ridList);

                } catch (Exception e) {
                    System.err.println("Error reading key " + i + " in node " + pageId + ": " + e.getMessage());
                    break;
                }
            }

            
        } catch (Exception e) {
            System.err.println("Error deserializing node " + pageId + ": " + e.getMessage());
            if (keys == null)
                keys = new ArrayList<>();
            if (values == null)
                values = new ArrayList<>();
        }
    }

    private byte[] serializeWithFewerKeys() {
        ByteBuffer buffer = ByteBuffer.allocate(PageImpl.PAGE_SIZE);

        buffer.putInt(pageId);
        buffer.put((byte) (isLeaf ? 1 : 0));
        buffer.putInt(parentPageId);

        int maxKeys = Math.min(keys.size(), 10);
        buffer.putInt(maxKeys);
        buffer.putInt(nextLeafPageId);

        for (int i = 0; i < maxKeys; i++) {
            K key = keys.get(i);
            String keyStr = key.toString();
            if (keyStr.length() > 20) {
                keyStr = keyStr.substring(0, 20);
            }
            byte[] keyBytes = keyStr.getBytes();

            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);

            List<Rid> ridList = values.get(i);
            int ridCount = Math.min(ridList.size(), 5);
            buffer.putInt(ridCount);

            for (int j = 0; j < ridCount; j++) {
                Rid rid = ridList.get(j);
                buffer.putInt(rid.getPageId());
                buffer.putInt(rid.getSlotId());
            }
        }

        return buffer.array();
    }
}