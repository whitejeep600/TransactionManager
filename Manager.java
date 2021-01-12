package cp1.solution;

import cp1.base.*;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class Manager implements TransactionManager {

    private final ConcurrentHashMap<ResourceId, Resource> resources;

    private final LocalTimeProvider timeProvider;

    private final ConcurrentHashMap<Thread, Transaction> transactionMap;
    // This map links threads to their currently performed transactions.

    private final ConcurrentHashMap<ResourceId, Transaction> resourceAccessingTransactions;
    // Informs which transaction (thread) currently has priority access to each resource
    // i. e. has accessed it within its current transaction.
    
    private boolean toBeAborted;

    public Manager(Collection<Resource> resources, LocalTimeProvider timeProvider){
        this.resources = new ConcurrentHashMap<>();
        for(Resource r: resources){
            this.resources.put(r.getId(), r);
        }
        this.timeProvider = timeProvider;
        this.transactionMap = new ConcurrentHashMap<>();
        resourceAccessingTransactions = new ConcurrentHashMap<>();
        toBeAborted = false;
    }

    private boolean resourcePresent(ResourceId rid){
            return resources.containsKey(rid);
    }

    private void grantAccess(ResourceId rid){
        Transaction cur = transactionMap.get(Thread.currentThread());
        cur.addResource(findResource(rid));
        resourceAccessingTransactions.put(rid,  cur);
    }

    private Thread executingThread(Transaction t){
        for(Thread th: transactionMap.keySet()){
            if(transactionMap.get(th).equals(t)){
                return th;
            }
        }
        return null;
    }

    private Transaction getCurrentTransaction(){
        return transactionMap.get(Thread.currentThread());
    }

    private Resource findResource(ResourceId rid){
        for(ResourceId r: resources.keySet()){
            if(r.equals(rid)) return resources.get(r);
        }
        return null;
    }

    // Grants a transaction priority access to a resource if the resource has not
    // been accessed by any other active transaction.
    private boolean grantIfFree(ResourceId rid){
            if(!resourceAccessingTransactions.containsKey(rid)){
                grantAccess(rid);
                return true;
            }
            return false;
    }

    private void freeTransactionResources(Transaction t){
        for(ResourceId r: t.getAccessedResources()){
            resourceAccessingTransactions.remove(r);
        }
    }

    private void endTransaction(Transaction t){
        freeTransactionResources(t);
        synchronized (transactionMap.get(Thread.currentThread())){
            transactionMap.remove(Thread.currentThread());
            for(ResourceId r: t.getAccessedResources()) {
                synchronized (resources.get(r)) {
                    resources.get(r).notifyAll();
                }
            }
        }
    }

    // Side note: a deadlock occurs if there is a cycle of transactions such that
    // each one of them is waiting for the next one in the cycle to free a resource.
    // Let us call such cycles 'deadlock-cycles'. The function below finds
    // the most recently started transaction in the deadlock-cycle that t belongs to.
    private Transaction findNewestInCycle(Transaction t){
        Transaction newest = t;
        Transaction aux = t.whoImWaitingFor();
        while(aux != t){
            if(aux.getStartTime() > newest.getStartTime()) newest = aux;
            else if(aux.getStartTime().equals(newest.getStartTime()) &&
                    executingThread(aux).getId() > executingThread(newest).getId()) newest = aux;
            aux = aux.whoImWaitingFor();
        }
        return newest;
    }

    // Checks if 'first' belongs to a deadlock-cycle (a well-known method of detecting cycles
    // in lists).
    private boolean deadlock(Transaction first){
        Transaction second = first;
        while (second != null){
            second = second.whoImWaitingFor();
            if(second == null) return false;
            second = second.whoImWaitingFor();
            if(second == null) return false;
            first = first.whoImWaitingFor();
            if(first.equals(second)) return true;
        }
        return false;
    }

    private void abortNewestIfDeadlock(ResourceId rid, Transaction current) throws ActiveTransactionAborted{
            Transaction next = resourceAccessingTransactions.get(rid);
            Transaction nextWaitsFor = next.whoImWaitingFor();
            current.setWhoImWaitingFor(resourceAccessingTransactions.get(rid));
            if(nextWaitsFor == null){
                return;
            }
            if(!deadlock(current)) return;
            Transaction t = findNewestInCycle(current);
            if(t.equals(current)){
                t.setAborted();
                Thread.currentThread().interrupt();
                throw new ActiveTransactionAborted();
            }
            for(Thread th: transactionMap.keySet()){
                if(transactionMap.get(th).equals(t)){
                    th.interrupt();
                    break;
                }
            }
            synchronized (t.whoImWaitingFor()) {
                toBeAborted = true;
                t.whoImWaitingFor().notify();
            }

    }

    @Override
    public void startTransaction() throws AnotherTransactionActiveException {
        if(isTransactionActive()){
            throw new AnotherTransactionActiveException();
        }
        else{
            transactionMap.put(Thread.currentThread(), new Transaction(timeProvider.getTime()));
        }
    }

    @Override
    public void commitCurrentTransaction() throws NoActiveTransactionException, ActiveTransactionAborted {
        if(!isTransactionActive()) throw new NoActiveTransactionException();
        if(isTransactionAborted()) throw new ActiveTransactionAborted();
        Transaction current = getCurrentTransaction();
        current.setCommitted();
        endTransaction(current);
    }

    @Override
    public void rollbackCurrentTransaction() {
        if(!isTransactionActive()) return;
        Transaction current = getCurrentTransaction();
        if(current.isCommitted()) return;
        current.rollBack();
        endTransaction(current);
    }

    @Override
    public boolean isTransactionActive() {
        return transactionMap.containsKey(Thread.currentThread());
    }

    @Override
    public boolean isTransactionAborted() {
        if(!transactionMap.containsKey(Thread.currentThread())) return false;
        return transactionMap.get(Thread.currentThread()).isAborted();
    }


    @Override
    public void operateOnResourceInCurrentTransaction(ResourceId rid, ResourceOperation operation)
            throws NoActiveTransactionException,
            UnknownResourceIdException,
            ActiveTransactionAborted,
            ResourceOperationException,
            InterruptedException {
        Transaction currentTransaction =  getCurrentTransaction();
        if(!isTransactionActive())throw new NoActiveTransactionException();
        else if(!resourcePresent(rid)) throw new UnknownResourceIdException(rid);
        else if(currentTransaction.isAborted()) throw new ActiveTransactionAborted();
        synchronized (resources.get(rid)) {
            if (!currentTransaction.hasAccessedResource(rid)) {
                boolean granted = grantIfFree(rid);
                if (!granted) {
                    abortNewestIfDeadlock(rid, currentTransaction);
                    while (resourceAccessingTransactions.containsKey(rid)) {
                        try {
                            currentTransaction.setWhoImWaitingFor(resourceAccessingTransactions.get(rid));
                            resources.get(rid).wait();
                        } catch (InterruptedException e) {
                            if (toBeAborted) {
                                getCurrentTransaction().setAborted();
                                toBeAborted = false;
                            }
                            throw new ActiveTransactionAborted();
                        }
                    }
                    grantAccess(rid);

                }
            }
        }
        currentTransaction.setWhoImWaitingFor(null);
        currentTransaction.execute(rid, operation);
        if(Thread.currentThread().isInterrupted()){
            throw new InterruptedException();
        }
    }
}
