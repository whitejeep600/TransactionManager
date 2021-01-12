package cp1.solution;

import cp1.base.ResourceId;
import cp1.base.Resource;
import cp1.base.ResourceOperation;
import cp1.base.ResourceOperationException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

public class Transaction {

    private final long startTime;
    private boolean aborted;
    private Transaction waitingFor;
    private final ConcurrentHashMap<ResourceId, ResourceAndOperations> modifiedResources;
    private boolean committed;

    public Transaction(long startTime){
        this.startTime = startTime;
        this.aborted = false;
        this.waitingFor = null;
        this.modifiedResources = new ConcurrentHashMap<>();
        this.committed = false;
    }

    public boolean isCommitted(){
        return committed;
    }

    public void setCommitted(){
        committed = true;
    }

    public Transaction whoImWaitingFor(){
        return waitingFor;
    }

    public Long getStartTime(){
        return startTime;
    }

    public void setAborted(){
        this.aborted = true;
    }

    public Set<ResourceId> getAccessedResources(){
        return this.modifiedResources.keySet();
    }

    public void setWhoImWaitingFor(Transaction t){
        waitingFor = t;
    }

    public boolean isAborted(){
        return aborted;
    }

    public void rollBack(){
        for(ResourceId rid: modifiedResources.keySet()){
            modifiedResources.get(rid).rollBackAll();
        }
    }

    public boolean hasAccessedResource(ResourceId rid){
        return modifiedResources.containsKey(rid);
    }

    public void addResource(Resource r){
        ResourceAndOperations rao = new ResourceAndOperations(r);
        modifiedResources.put(r.getId(), rao);
    }

    public void execute(ResourceId rid, ResourceOperation ro) throws ResourceOperationException {
        modifiedResources.get(rid).execute(ro);
    }

}
