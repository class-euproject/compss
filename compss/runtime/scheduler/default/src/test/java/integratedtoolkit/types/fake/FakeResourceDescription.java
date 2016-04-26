package integratedtoolkit.types.fake;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.resources.ResourceDescription;

public class FakeResourceDescription extends ResourceDescription {

    private int coreCount = 0;

    public FakeResourceDescription(int coreCount) {
        this.coreCount = coreCount;
    }

    @Override
    public boolean canHost(Implementation<?> impl) {
        FakeResourceDescription desc = (FakeResourceDescription) impl.getRequirements();
        return canHost(desc);
    }

    public boolean canHost(FakeResourceDescription desc) {
        return !(desc.coreCount > this.coreCount);
    }

    @Override
    public void increase(ResourceDescription rd) {
        FakeResourceDescription desc = (FakeResourceDescription) rd;
        this.coreCount += desc.coreCount;
    }

    @Override
    public void reduce(ResourceDescription rd) {
        FakeResourceDescription desc = (FakeResourceDescription) rd;
        this.coreCount -= desc.coreCount;
    }

    @Override
    public void increaseDynamic(ResourceDescription rd) {
        FakeResourceDescription desc = (FakeResourceDescription) rd;
        this.coreCount += desc.coreCount;
    }

    @Override
    public void reduceDynamic(ResourceDescription rd) {
        FakeResourceDescription desc = (FakeResourceDescription) rd;
        this.coreCount -= desc.coreCount;
    }

    @Override
    public boolean isDynamicUseless() {
        return coreCount == 0;
    }

    @Override
    public ResourceDescription getDynamicCommons(ResourceDescription other) {
        FakeResourceDescription otherFake = (FakeResourceDescription) other;
        int coreCommons = Math.min(coreCount, otherFake.coreCount);
        return new FakeResourceDescription(coreCommons);
    }

    @Override
    public ResourceDescription copy() {
        return new FakeResourceDescription(coreCount);
    }

    public String toString() {
        return coreCount + " cores";
    }

    public boolean checkEquals(FakeResourceDescription fakeResourceDescription) {
        return coreCount == fakeResourceDescription.coreCount;
    }

}
