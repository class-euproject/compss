package integratedtoolkit.scheduler.readyscheduler;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.util.ResourceScheduler;

public class ReadyResourceScheduler extends ResourceScheduler<Profile> {

    public ReadyResourceScheduler(Worker w) {
        super(w);
    }

    /*
     * It filters the implementations that can be executed at the moment. If 
     * there are no available resources in the worker to host the implementation 
     * execution, it ignores the implementation.
     */
    @Override
    public Score getImplementationScore(AllocatableAction action, TaskParams params, Implementation impl, Score resourceScore) {
        if (myWorker.canRunNow(impl.getRequirements())) {
            long implScore = this.getProfile(impl).getAverageExecutionTime();
            return new Score(resourceScore, implScore);
        } else {
            return null;
        }
    }

    /*
     * It only receives actions whose execution the worker can host at the moment.
     * Same behaviour as the base case. Not adding any resource dependency.
     *
     * @Override
     * public void initialSchedule(AllocatableAction action, Implementation bestImpl) {
     *    //No need to add any resource dependency. It can start executing!
     *
     *   }
     */
}
