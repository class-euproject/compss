package dynamicTest;

import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.annotations.Method;


public interface DynamicTestItf {
	
	@Method(declaringClass = "dynamicTest.DynamicTestImpl")
	@Constraints(operatingSystemType = "Windows", computingUnits = 3)
	void coreElementDynamic1();
	
	@Method(declaringClass = "dynamicTest.DynamicTestImpl")
	@Constraints(operatingSystemType = "Windows", computingUnits = 1)
	void coreElementDynamic2();
}
