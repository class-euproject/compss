package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskDispatcher.TaskProducer;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.allocatableactions.SingleExecution;
import integratedtoolkit.types.request.exceptions.ShutdownException;

/**
 * The ExecuteTasksRequest class represents the request to execute a group of
 * dependency-free tasks.
 */
public class ExecuteTasksRequest extends TDRequest {

    private final TaskProducer producer;
    /**
     * Task to run
     */
    private final Task task;

    /**
     * Constructs a new ScheduleTasks Request
     *
     * @param producer taskProducer to be notified when the task ends
     * @param t Task to run
     */
    public ExecuteTasksRequest(TaskProducer producer, Task t) {
        this.producer = producer;
        this.task = t;
    }

    /**
     * Returns the task to execute
     *
     * @return task to execute
     */
    public Task getTask() {
        return task;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        int coreID = task.getTaskParams().getId();
        if (debug) {
            logger.debug("Treating Scheduling request for task " + task.getId() + "(core " + coreID + ")");
        }
        task.setStatus(Task.TaskState.TO_EXECUTE);

        SingleExecution e = new SingleExecution(ts.generateSchedulingInformation(), producer, task);
        ts.newAllocatableAction(e);
    }

}
