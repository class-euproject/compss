package es.bsc.compss.executor.external.piped.commands;

import es.bsc.compss.executor.external.commands.CancelJobExternalCommand;
import es.bsc.compss.executor.external.piped.PipePair;


public class CancelJobCommand extends CancelJobExternalCommand implements PipeCommand {

    public PipePair pipe;


    public CancelJobCommand(PipePair pipe) {
        this.pipe = pipe;
    }

    public CancelJobCommand() {

    }

    @Override
    public String getAsString() {
        StringBuilder sb = new StringBuilder(super.getAsString());
        sb.append(" ").append(pipe.getOutboundPipe()).append(" ").append(pipe.getInboundPipe());
        return sb.toString();
    }

    @Override
    public int compareTo(PipeCommand t) {
        int value = Integer.compare(this.getType().ordinal(), t.getType().ordinal());
        if (value != 0) {
            value = pipe.getPipesLocation().compareTo(((CancelJobCommand) t).pipe.getPipesLocation());
        }
        return value;
    }

    @Override
    public void join(PipeCommand receivedCommand) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
