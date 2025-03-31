package buffermanager;

public class Rid {
    private int pageId;
    private int slotId;
    
    public Rid(int pageId, int slotId) {
        this.pageId = pageId;
        this.slotId = slotId;
    }
    
    public int getPageId() {
        return pageId;
    }
    
    public int getSlotId() {
        return slotId;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Rid rid = (Rid) obj;
        return pageId == rid.pageId && slotId == rid.slotId;
    }
    
    @Override
    public int hashCode() {
        return 31 * pageId + slotId;
    }
    
    @Override
    public String toString() {
        return "Rid{" +
                "pageId=" + pageId +
                ", slotId=" + slotId +
                '}';
    }
}