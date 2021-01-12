package cp1.solution;

import cp1.base.Resource;
import cp1.base.ResourceOperation;
import cp1.base.ResourceOperationException;
import java.util.Vector;

// A class which stores a resource and its history of operations perforemd within
// a given transaction.
public class ResourceAndOperations {
    private final Resource r;
    private final Vector<ResourceOperation> operations;

    public ResourceAndOperations(Resource r) {
        this.r = r;
        this.operations = new Vector<>();
    }

    public void execute(ResourceOperation ro) throws ResourceOperationException{
        ro.execute(r);
        operations.add(ro);
    }

    public void rollBackAll() {
        for (int i = operations.size()-1; i >= 0; i--) {
            operations.get(i).undo(r);
        }
    }
}
