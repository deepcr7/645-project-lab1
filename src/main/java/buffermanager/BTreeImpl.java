package buffermanager;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BTreeImpl<K extends Comparable<K>> implements BTree<K, Rid> {
    private static final int ORDER = 200;
    private final ExtendedBufferManager bufferManager;
    private final String indexFilename;
    private int rootPageId;
    private boolean isBulkLoading;
    private K lastBulkKey;

    public BTreeImpl(ExtendedBufferManager bufferManager, String indexFilename) {
        this.bufferManager = bufferManager;
        this.indexFilename = indexFilename;
        this.rootPageId = -1;
        this.isBulkLoading = false;
        initialize();
    }

    private void initialize() {
        File indexFile = new File(indexFilename);
        if (indexFile.exists() && indexFile.length() > 0) {
            Page page = bufferManager.getPage(indexFilename, 0);
            if (page != null) {
                rootPageId = 0;
                bufferManager.unpinPage(indexFilename, 0);
                return;
            }
        }

        Page page = bufferManager.createPage(indexFilename);
        if (page == null) {
            throw new RuntimeException("Failed to create root page for B+tree");
        }
        rootPageId = page.getPid();
        BTreeLeafNode<K> rootNode = new BTreeLeafNode<>(rootPageId);
        saveNode(rootNode);
        bufferManager.unpinPage(indexFilename, rootPageId);
    }

    @Override
    public void insert(K key, Rid rid) {

        if (isBulkLoading) {
            if (lastBulkKey != null && key.compareTo(lastBulkKey) < 0) {
                throw new IllegalArgumentException("Bulk loading requires sorted keys");
            }

            lastBulkKey = key;
            bulkInsert(key, rid);
        } else {
            normalInsert(key, rid);
        }
    }

    public void startBulkLoading() {
        isBulkLoading = true;
        lastBulkKey = null;
    }

    public void endBulkLoading() {
        isBulkLoading = false;
        lastBulkKey = null;
    }

    private void bulkInsert(K key, Rid rid) {
        try {
            BTreeNode<K> node = getRightmostLeafNode();
            BTreeLeafNode<K> leaf = (BTreeLeafNode<K>) node;

            boolean keyExists = false;
            int i;
            for (i = 0; i < leaf.getKeys().size(); i++) {
                int comparison = key.compareTo(leaf.getKeys().get(i));
                if (comparison == 0) {
                    leaf.getValues().get(i).add(rid);
                    keyExists = true;
                    break;
                } else if (comparison < 0) {
                    break;
                }
            }

            if (!keyExists) {
                if (leaf.getKeys().size() >= ORDER - 1) {
                    splitLeafNodeForBulk(leaf, key, rid);
                } else {
                    leaf.getKeys().add(i, key);
                    List<Rid> ridList = new ArrayList<>();
                    ridList.add(rid);
                    leaf.getValues().add(i, ridList);

                    saveNode(leaf);
                }
            } else {
                saveNode(leaf);
            }

            bufferManager.unpinPage(indexFilename, leaf.getPageId());
        } catch (Exception e) {
            System.err.println("Error in bulkInsert: " + e.getMessage());
            e.printStackTrace();
            normalInsert(key, rid);
        }
    }

    private void normalInsert(K key, Rid rid) {
        try {

            BTreeNode<K> node = findLeafNode(key);
            if (node == null) {
                Page page = bufferManager.createPage(indexFilename);
                if (page == null) {
                    throw new RuntimeException("Failed to create root page for B+tree");
                }
                rootPageId = page.getPid();
                node = new BTreeLeafNode<>(rootPageId);
            }

            BTreeLeafNode<K> leaf = (BTreeLeafNode<K>) node;

            int insertPos = 0;
            while (insertPos < leaf.getKeys().size() && key.compareTo(leaf.getKeys().get(insertPos)) > 0) {
                insertPos++;
            }

            if (insertPos < leaf.getKeys().size() && key.compareTo(leaf.getKeys().get(insertPos)) == 0) {
                leaf.getValues().get(insertPos).add(rid);
            } else {
                leaf.getKeys().add(insertPos, key);
                List<Rid> ridList = new ArrayList<>();
                ridList.add(rid);
                leaf.getValues().add(insertPos, ridList);
            }

            saveNode(leaf);

            if (leaf.getKeys().size() >= ORDER) {
                splitLeafNode(leaf);
            } else {
                bufferManager.unpinPage(indexFilename, leaf.getPageId());
            }
        } catch (Exception e) {
            System.err.println("ERROR in normalInsert: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void splitLeafNode(BTreeLeafNode<K> node) {
        try {
            bufferManager.force(indexFilename);

            for (int i = 0; i < 5; i++) {
                bufferManager.unpinPage(indexFilename, node.getPageId());
            }

            if (bufferManager instanceof ExtendedBufferManagerImpl) {
                ((ExtendedBufferManagerImpl) bufferManager).freeUpBufferSpace();
            }

            Page newPage = bufferManager.createPage(indexFilename);
            if (newPage == null) {
                if (bufferManager instanceof ExtendedBufferManagerImpl) {
                    ((ExtendedBufferManagerImpl) bufferManager).aggressiveCleanup();
                }

                newPage = bufferManager.createPage(indexFilename);
                if (newPage == null) {
                    throw new RuntimeException("Failed to create new page for B+ tree node split");
                }
            }

            int newPageId = newPage.getPid();
            BTreeLeafNode<K> newNode = new BTreeLeafNode<>(newPageId);

            int mid = node.getKeys().size() / 2;

            for (int i = mid; i < node.getKeys().size(); i++) {
                newNode.getKeys().add(node.getKeys().get(i));
                newNode.getValues().add(node.getValues().get(i));
            }

            List<K> originalKeys = new ArrayList<>(node.getKeys().subList(0, mid));
            List<List<Rid>> originalValues = new ArrayList<>(node.getValues().subList(0, mid));

            node.getKeys().clear();
            node.getValues().clear();

            node.getKeys().addAll(originalKeys);
            node.getValues().addAll(originalValues);

            newNode.setNextLeafPageId(node.getNextLeafPageId());
            node.setNextLeafPageId(newNode.getPageId());

            K midKey = newNode.getKeys().get(0);

            if (node.getPageId() == rootPageId) {
                saveNode(node);
                saveNode(newNode);

                Page rootPage = bufferManager.createPage(indexFilename);
                if (rootPage == null) {
                    throw new RuntimeException("Failed to create new root page for B+ tree");
                }

                int newRootPageId = rootPage.getPid();
                BTreeInternalNode<K> newRoot = new BTreeInternalNode<>(newRootPageId);

                newRoot.getKeys().add(midKey);
                newRoot.getChildrenPageIds().add(node.getPageId());
                newRoot.getChildrenPageIds().add(newNode.getPageId());

                node.setParentPageId(newRoot.getPageId());
                newNode.setParentPageId(newRoot.getPageId());

                rootPageId = newRoot.getPageId();

                saveNode(newRoot);
                saveNode(node);
                saveNode(newNode);

                bufferManager.unpinPage(indexFilename, newRoot.getPageId());
                bufferManager.unpinPage(indexFilename, node.getPageId());
                bufferManager.unpinPage(indexFilename, newNode.getPageId());
            } else {
                newNode.setParentPageId(node.getParentPageId());

                saveNode(node);
                saveNode(newNode);

                bufferManager.unpinPage(indexFilename, node.getPageId());
                bufferManager.unpinPage(indexFilename, newNode.getPageId());

                insertIntoParentIterative(node, midKey, newNode);
            }
        } catch (Exception e) {
            System.err.println("Error in splitLeafNode: " + e.getMessage());
            e.printStackTrace();
            bufferManager.unpinPage(indexFilename, node.getPageId());
            throw new RuntimeException("Failed to split leaf node: " + e.getMessage());
        }
    }

    private void insertIntoParent(BTreeNode<K> leftNode, K key, BTreeNode<K> rightNode) {

        BTreeNode<K> parent = getNode(leftNode.getParentPageId());
        BTreeInternalNode<K> parentNode = (BTreeInternalNode<K>) parent;

        int insertPos = 0;
        while (insertPos < parentNode.getKeys().size() &&
                key.compareTo(parentNode.getKeys().get(insertPos)) > 0) {
            insertPos++;
        }

        parentNode.getKeys().add(insertPos, key);
        parentNode.getChildrenPageIds().add(insertPos + 1, rightNode.getPageId());

        rightNode.setParentPageId(parentNode.getPageId());

        if (parentNode.getKeys().size() >= ORDER) {
            splitInternalNode(parentNode);
        } else {

            saveNode(parentNode);
        }

        bufferManager.unpinPage(indexFilename, parentNode.getPageId());
    }

    private void splitInternalNode(BTreeInternalNode<K> node) {

        Page newPage = bufferManager.createPage(indexFilename);
        if (newPage == null) {

            bufferManager.unpinPage(indexFilename, node.getPageId());

            newPage = bufferManager.createPage(indexFilename);
            if (newPage == null) {
                throw new RuntimeException("Failed to create new page for B+ tree node split");
            }
        }

        int newPageId = newPage.getPid();
        BTreeInternalNode<K> newNode = new BTreeInternalNode<>(newPageId);

        int mid = node.getKeys().size() / 2;
        K midKey = node.getKeys().get(mid);

        for (int i = mid + 1; i < node.getKeys().size(); i++) {
            newNode.getKeys().add(node.getKeys().get(i));
        }

        for (int i = mid + 1; i < node.getChildrenPageIds().size(); i++) {
            newNode.getChildrenPageIds().add(node.getChildrenPageIds().get(i));
        }

        List<K> originalKeys = new ArrayList<>(node.getKeys().subList(0, mid));
        List<Integer> originalChildren = new ArrayList<>(node.getChildrenPageIds().subList(0, mid + 1));

        node.getKeys().clear();
        node.getChildrenPageIds().clear();

        node.getKeys().addAll(originalKeys);
        node.getChildrenPageIds().addAll(originalChildren);

        for (int childPageId : newNode.getChildrenPageIds()) {
            BTreeNode<K> childNode = getNode(childPageId);
            childNode.setParentPageId(newNode.getPageId());
            saveNode(childNode);
            bufferManager.unpinPage(indexFilename, childPageId);
        }

        if (node.getPageId() == rootPageId) {
            Page rootPage = bufferManager.createPage(indexFilename);
            if (rootPage == null) {
                throw new RuntimeException("Failed to create new root page for B+ tree");
            }

            int newRootPageId = rootPage.getPid();
            BTreeInternalNode<K> newRoot = new BTreeInternalNode<>(newRootPageId);

            newRoot.getKeys().add(midKey);
            newRoot.getChildrenPageIds().add(node.getPageId());
            newRoot.getChildrenPageIds().add(newNode.getPageId());

            node.setParentPageId(newRoot.getPageId());
            newNode.setParentPageId(newRoot.getPageId());

            rootPageId = newRoot.getPageId();

            saveNode(newRoot);
            saveNode(node);
            saveNode(newNode);

            bufferManager.unpinPage(indexFilename, newRoot.getPageId());
        } else {

            newNode.setParentPageId(node.getParentPageId());
            insertIntoParent(node, midKey, newNode);

            saveNode(node);
            saveNode(newNode);
        }

        bufferManager.unpinPage(indexFilename, newNode.getPageId());
    }

    private void insertIntoParentIterative(BTreeNode<K> leftNode, K key, BTreeNode<K> rightNode) {
        try {

            saveNode(leftNode);
            saveNode(rightNode);

            int parentId = leftNode.getParentPageId();
            rightNode.setParentPageId(parentId);

            if (parentId < 0) {

                Page rootPage = bufferManager.createPage(indexFilename);
                if (rootPage == null) {
                    throw new RuntimeException("Failed to create new root page for B+ tree");
                }

                int newRootPageId = rootPage.getPid();
                BTreeInternalNode<K> newRoot = new BTreeInternalNode<>(newRootPageId);

                newRoot.getKeys().add(key);
                newRoot.getChildrenPageIds().add(leftNode.getPageId());
                newRoot.getChildrenPageIds().add(rightNode.getPageId());

                leftNode.setParentPageId(newRoot.getPageId());
                rightNode.setParentPageId(newRoot.getPageId());

                rootPageId = newRoot.getPageId();

                saveNode(newRoot);
                saveNode(leftNode);
                saveNode(rightNode);

                bufferManager.unpinPage(indexFilename, newRoot.getPageId());
                bufferManager.unpinPage(indexFilename, leftNode.getPageId());
                bufferManager.unpinPage(indexFilename, rightNode.getPageId());

                return;
            }

            bufferManager.unpinPage(indexFilename, leftNode.getPageId());
            bufferManager.unpinPage(indexFilename, rightNode.getPageId());

            Page parentPage = bufferManager.getPage(indexFilename, parentId);
            if (parentPage == null) {
                throw new RuntimeException("Failed to load parent page in B+ tree");
            }

            BTreeInternalNode<K> parentNode = new BTreeInternalNode<>(parentId);
            parentNode.deserialize(parentPage.getData());

            int insertPos = 0;
            while (insertPos < parentNode.getKeys().size() &&
                    key.compareTo(parentNode.getKeys().get(insertPos)) > 0) {
                insertPos++;
            }

            parentNode.getKeys().add(insertPos, key);
            parentNode.getChildrenPageIds().add(insertPos + 1, rightNode.getPageId());

            if (parentNode.getKeys().size() >= ORDER) {

                splitInternalNode(parentNode);
            } else {

                saveNode(parentNode);
                bufferManager.unpinPage(indexFilename, parentId);
            }
        } catch (Exception e) {
            System.err.println("Error in insertIntoParentIterative: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to insert into parent: " + e.getMessage());
        }
    }

    @Override
    public Iterator<Rid> search(K key) {
        try {
            BTreeNode<K> node = findLeafNode(key);
            if (node == null) {
                return Collections.emptyIterator();
            }

            BTreeLeafNode<K> leaf = (BTreeLeafNode<K>) node;
            for (int i = 0; i < leaf.getKeys().size(); i++) {
                if (key.compareTo(leaf.getKeys().get(i)) == 0) {
                    List<Rid> results = new ArrayList<>(leaf.getValues().get(i));
                    bufferManager.unpinPage(indexFilename, leaf.getPageId());
                    return results.iterator();
                }
            }

            bufferManager.unpinPage(indexFilename, leaf.getPageId());
            return Collections.emptyIterator();
        } catch (Exception e) {
            System.err.println("Error in search: " + e.getMessage());
            e.printStackTrace();
            return Collections.emptyIterator();
        }
    }

    @Override
    public Iterator<Rid> rangeSearch(K startKey, K endKey) {
        List<Rid> results = new ArrayList<>();

        try {
            BTreeNode<K> startNode = findLeafNode(startKey);

            if (startNode == null) {
                return results.iterator();
            }

            BTreeLeafNode<K> leaf = (BTreeLeafNode<K>) startNode;
            boolean done = false;
            while (!done && leaf != null) {
                for (int i = 0; i < leaf.getKeys().size(); i++) {
                    K key = leaf.getKeys().get(i);
                    if ((key.compareTo(startKey) >= 0) && (key.compareTo(endKey) <= 0)) {
                        List<Rid> ridList = leaf.getValues().get(i);
                        results.addAll(ridList);
                    }
                    if (key.compareTo(endKey) > 0) {
                        done = true;
                        break;
                    }
                }
                if (!done && leaf.getNextLeafPageId() != -1) {
                    int nextPageId = leaf.getNextLeafPageId();
                    bufferManager.unpinPage(indexFilename, leaf.getPageId());
                    BTreeNode<K> nextNode = getNode(nextPageId);
                    if (nextNode != null && nextNode.isLeaf()) {
                        leaf = (BTreeLeafNode<K>) nextNode;
                    } else {
                        leaf = null;
                    }
                } else {
                    bufferManager.unpinPage(indexFilename, leaf.getPageId());
                    leaf = null;
                }
            }

        } catch (Exception e) {
            System.err.println("Error in range search: " + e.getMessage());
            e.printStackTrace();
        }

        return results.iterator();
    }

    private class RangeIterator implements Iterator<Rid> {
        private K startKey;
        private K endKey;
        private BTreeLeafNode<K> currentLeaf;
        private int currentKeyIndex;
        private int currentRidIndex;
        private List<Rid> currentRids;
        private boolean hasMore;
        private boolean initialized;

        public RangeIterator(K startKey, K endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
            this.initialized = false;
            this.hasMore = true;
        }

        private void initialize() {
            if (initialized)
                return;

            BTreeNode<K> node = findLeafNode(startKey);
            if (node == null) {
                hasMore = false;
                return;
            }

            this.currentLeaf = (BTreeLeafNode<K>) node;

            this.currentKeyIndex = 0;
            while (currentKeyIndex < currentLeaf.getKeys().size() &&
                    startKey.compareTo(currentLeaf.getKeys().get(currentKeyIndex)) > 0) {
                currentKeyIndex++;
            }

            if (currentKeyIndex >= currentLeaf.getKeys().size()) {
                if (currentLeaf.getNextLeafPageId() == -1) {

                    hasMore = false;
                    bufferManager.unpinPage(indexFilename, currentLeaf.getPageId());
                    return;
                }

                int nextLeafId = currentLeaf.getNextLeafPageId();
                bufferManager.unpinPage(indexFilename, currentLeaf.getPageId());

                Page nextPage = bufferManager.getPage(indexFilename, nextLeafId);
                if (nextPage == null) {
                    hasMore = false;
                    return;
                }

                BTreeNode<K> nextNode = new BTreeLeafNode<>(nextLeafId);
                nextNode.deserialize(nextPage.getData());
                currentLeaf = (BTreeLeafNode<K>) nextNode;
                currentKeyIndex = 0;
            }

            if (currentLeaf.getKeys().get(currentKeyIndex).compareTo(endKey) > 0) {
                hasMore = false;
                bufferManager.unpinPage(indexFilename, currentLeaf.getPageId());
                return;
            }

            currentRids = currentLeaf.getValues().get(currentKeyIndex);
            currentRidIndex = 0;

            initialized = true;
        }

        @Override
        public boolean hasNext() {
            if (!initialized) {
                initialize();
            }

            return hasMore && currentRids != null && currentRidIndex < currentRids.size();
        }

        @Override
        public Rid next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Rid result = currentRids.get(currentRidIndex);
            currentRidIndex++;

            if (currentRidIndex >= currentRids.size()) {
                moveToNextKey();
            }

            return result;
        }

        private void moveToNextKey() {
            currentKeyIndex++;

            while (currentKeyIndex >= currentLeaf.getKeys().size()) {
                if (currentLeaf.getNextLeafPageId() == -1) {

                    hasMore = false;
                    bufferManager.unpinPage(indexFilename, currentLeaf.getPageId());
                    return;
                }

                int nextLeafId = currentLeaf.getNextLeafPageId();
                bufferManager.unpinPage(indexFilename, currentLeaf.getPageId());

                Page nextPage = bufferManager.getPage(indexFilename, nextLeafId);
                if (nextPage == null) {
                    hasMore = false;
                    return;
                }

                BTreeNode<K> nextNode = new BTreeLeafNode<>(nextLeafId);
                nextNode.deserialize(nextPage.getData());
                currentLeaf = (BTreeLeafNode<K>) nextNode;
                currentKeyIndex = 0;
            }

            if (currentLeaf.getKeys().get(currentKeyIndex).compareTo(endKey) > 0) {
                hasMore = false;
                bufferManager.unpinPage(indexFilename, currentLeaf.getPageId());
                return;
            }

            currentRids = currentLeaf.getValues().get(currentKeyIndex);
            currentRidIndex = 0;
        }
    }

    private BTreeNode<K> findLeafNode(K key) {
        try {
            if (rootPageId == -1) {
                return null;
            }

            BTreeNode<K> node = getNode(rootPageId);
            if (node == null) {
                return null;
            }

            while (!node.isLeaf()) {
                BTreeInternalNode<K> internalNode = (BTreeInternalNode<K>) node;

                int index = 0;
                while (index < internalNode.getKeys().size() &&
                        key.compareTo(internalNode.getKeys().get(index)) >= 0) {
                    index++;
                }

                int childPageId = internalNode.getChildrenPageIds().get(index);

                bufferManager.unpinPage(indexFilename, node.getPageId());
                node = getNode(childPageId);

                if (node == null) {
                    return null;
                }
            }

            return node;
        } catch (Exception e) {
            System.err.println("ERROR in findLeafNode: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private BTreeNode<K> getNode(int pageId) {
        try {
            Page page = bufferManager.getPage(indexFilename, pageId);
            if (page == null) {
                System.err.println("GET NODE - Failed to load page: " + pageId);
                return null;
            }

            byte[] data = page.getData();

            boolean emptyPage = true;
            for (int i = 0; i < 20 && i < data.length; i++) {
                if (data[i] != 0) {
                    emptyPage = false;
                    break;
                }
            }

            if (emptyPage) {
            }

            boolean isLeaf = data[4] == 1;
            BTreeNode<K> node;
            if (isLeaf) {
                node = new BTreeLeafNode<>(pageId);
            } else {
                node = new BTreeInternalNode<>(pageId);
            }

            try {
                node.deserialize(data);

            } catch (Exception e) {
                System.err.println("GET NODE - Error deserializing node: " + e.getMessage());
                e.printStackTrace();
            }

            return node;
        } catch (Exception e) {
            System.err.println("ERROR in getNode: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private BTreeNode<K> getRightmostLeafNode() {
        if (rootPageId == -1) {
            Page page = bufferManager.createPage(indexFilename);
            if (page == null) {
                throw new RuntimeException("Failed to create root page for B+ tree");
            }

            rootPageId = page.getPid();
            BTreeLeafNode<K> root = new BTreeLeafNode<>(rootPageId);
            saveNode(root);

            return root;
        }

        BTreeNode<K> node = getNode(rootPageId);

        while (!node.isLeaf()) {
            BTreeInternalNode<K> internalNode = (BTreeInternalNode<K>) node;
            List<Integer> children = internalNode.getChildrenPageIds();
            int lastChildId = children.get(children.size() - 1);

            bufferManager.unpinPage(indexFilename, node.getPageId());

            node = getNode(lastChildId);
        }

        return node;
    }

    private void splitLeafNodeForBulk(BTreeLeafNode<K> leaf, K newKey, Rid newRid) {

        Page newPage = bufferManager.createPage(indexFilename);
        if (newPage == null) {
            if (bufferManager instanceof ExtendedBufferManagerImpl) {
                ((ExtendedBufferManagerImpl) bufferManager).freeUpBufferSpace();
            }
            newPage = bufferManager.createPage(indexFilename);
            if (newPage == null) {
                throw new RuntimeException("Failed to create new leaf page during bulk loading");
            }
        }

        int newPageId = newPage.getPid();
        BTreeLeafNode<K> newLeaf = new BTreeLeafNode<>(newPageId);

        newLeaf.getKeys().add(newKey);
        List<Rid> ridList = new ArrayList<>();
        ridList.add(newRid);
        newLeaf.getValues().add(ridList);

        newLeaf.setNextLeafPageId(leaf.getNextLeafPageId());
        leaf.setNextLeafPageId(newLeaf.getPageId());

        K separatorKey = newKey;

        if (leaf.getParentPageId() == -1) {

            Page rootPage = bufferManager.createPage(indexFilename);
            if (rootPage == null) {
                throw new RuntimeException("Failed to create new root page");
            }

            int newRootPageId = rootPage.getPid();
            BTreeInternalNode<K> newRoot = new BTreeInternalNode<>(newRootPageId);

            newRoot.getKeys().add(separatorKey);
            newRoot.getChildrenPageIds().add(leaf.getPageId());
            newRoot.getChildrenPageIds().add(newLeaf.getPageId());

            leaf.setParentPageId(newRoot.getPageId());
            newLeaf.setParentPageId(newRoot.getPageId());

            rootPageId = newRoot.getPageId();

            saveNode(newRoot);
            saveNode(leaf);
            saveNode(newLeaf);
            bufferManager.unpinPage(indexFilename, newRoot.getPageId());
        } else {

            int parentId = leaf.getParentPageId();
            newLeaf.setParentPageId(parentId);

            Page parentPage = bufferManager.getPage(indexFilename, parentId);
            if (parentPage == null) {
                throw new RuntimeException("Failed to load parent page");
            }

            BTreeInternalNode<K> parent = new BTreeInternalNode<>(parentId);
            parent.deserialize(parentPage.getData());

            parent.getKeys().add(separatorKey);
            parent.getChildrenPageIds().add(newLeaf.getPageId());

            if (parent.getKeys().size() >= ORDER) {
                splitInternalNodeForBulk(parent);
            } else {
                saveNode(parent);
                bufferManager.unpinPage(indexFilename, parentId);
            }
            saveNode(leaf);
            saveNode(newLeaf);
        }

        bufferManager.unpinPage(indexFilename, newLeaf.getPageId());
    }

    private void saveNode(BTreeNode<K> node) {
        try {
            Page page = bufferManager.getPage(indexFilename, node.getPageId());
            if (page == null) {
                throw new RuntimeException("Failed to get page for node: " + node.getPageId());
            }

            byte[] data = node.serialize();
            page.setData(data);
            bufferManager.markDirty(indexFilename, node.getPageId());

            bufferManager.unpinPage(indexFilename, node.getPageId());

            if (node.isLeaf()) {
                BTreeLeafNode<K> leaf = (BTreeLeafNode<K>) node;
            } else {
                BTreeInternalNode<K> internal = (BTreeInternalNode<K>) node;
                System.out.println(
                        "Saved internal node " + node.getPageId() + " with " + internal.getKeys().size() + " keys");
            }
        } catch (Exception e) {
            System.err.println("Error saving node " + node.getPageId() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void splitInternalNodeForBulk(BTreeInternalNode<K> node) {

        Page newPage = bufferManager.createPage(indexFilename);
        if (newPage == null) {
            if (bufferManager instanceof ExtendedBufferManagerImpl) {
                ((ExtendedBufferManagerImpl) bufferManager).freeUpBufferSpace();
            }
            newPage = bufferManager.createPage(indexFilename);
            if (newPage == null) {
                throw new RuntimeException("Failed to create new internal node during bulk loading");
            }
        }

        int newPageId = newPage.getPid();
        BTreeInternalNode<K> newNode = new BTreeInternalNode<>(newPageId);

        int mid = node.getKeys().size() / 2;
        K midKey = node.getKeys().get(mid);

        for (int i = mid + 1; i < node.getKeys().size(); i++) {
            newNode.getKeys().add(node.getKeys().get(i));
        }

        for (int i = mid + 1; i <= node.getChildrenPageIds().size() - 1; i++) {
            newNode.getChildrenPageIds().add(node.getChildrenPageIds().get(i));
        }

        for (int childId : newNode.getChildrenPageIds()) {
            Page childPage = bufferManager.getPage(indexFilename, childId);
            if (childPage != null) {
                BTreeNode<K> childNode;
                if (childPage.getData()[4] == 1) {
                    childNode = new BTreeLeafNode<>(childId);
                } else {
                    childNode = new BTreeInternalNode<>(childId);
                }
                childNode.deserialize(childPage.getData());
                childNode.setParentPageId(newNode.getPageId());
                saveNode(childNode);
                bufferManager.unpinPage(indexFilename, childId);
            }
        }

        List<K> originalKeys = new ArrayList<>(node.getKeys().subList(0, mid));
        List<Integer> originalChildren = new ArrayList<>(node.getChildrenPageIds().subList(0, mid + 1));

        node.getKeys().clear();
        node.getChildrenPageIds().clear();

        node.getKeys().addAll(originalKeys);
        node.getChildrenPageIds().addAll(originalChildren);

        if (node.getPageId() == rootPageId) {

            Page rootPage = bufferManager.createPage(indexFilename);
            if (rootPage == null) {
                throw new RuntimeException("Failed to create new root page");
            }

            int newRootPageId = rootPage.getPid();
            BTreeInternalNode<K> newRoot = new BTreeInternalNode<>(newRootPageId);

            newRoot.getKeys().add(midKey);
            newRoot.getChildrenPageIds().add(node.getPageId());
            newRoot.getChildrenPageIds().add(newNode.getPageId());

            node.setParentPageId(newRoot.getPageId());
            newNode.setParentPageId(newRoot.getPageId());

            rootPageId = newRoot.getPageId();
            saveNode(newRoot);
            saveNode(node);
            saveNode(newNode);

            bufferManager.unpinPage(indexFilename, newRoot.getPageId());
            bufferManager.unpinPage(indexFilename, node.getPageId());
            bufferManager.unpinPage(indexFilename, newNode.getPageId());
        } else {
            int parentId = node.getParentPageId();
            newNode.setParentPageId(parentId);

            Page parentPage = bufferManager.getPage(indexFilename, parentId);
            if (parentPage == null) {
                throw new RuntimeException("Failed to load parent page");
            }

            BTreeInternalNode<K> parent = new BTreeInternalNode<>(parentId);
            parent.deserialize(parentPage.getData());

            int insertPos = 0;
            while (insertPos < parent.getKeys().size() &&
                    midKey.compareTo(parent.getKeys().get(insertPos)) > 0) {
                insertPos++;
            }

            parent.getKeys().add(insertPos, midKey);
            parent.getChildrenPageIds().add(insertPos + 1, newNode.getPageId());

            if (parent.getKeys().size() >= ORDER) {

                saveNode(node);
                saveNode(newNode);
                bufferManager.unpinPage(indexFilename, node.getPageId());
                bufferManager.unpinPage(indexFilename, newNode.getPageId());

                splitInternalNodeForBulk(parent);
            } else {
                saveNode(parent);
                saveNode(node);
                saveNode(newNode);
                bufferManager.unpinPage(indexFilename, parentId);
                bufferManager.unpinPage(indexFilename, node.getPageId());
                bufferManager.unpinPage(indexFilename, newNode.getPageId());
            }
        }
    }
}