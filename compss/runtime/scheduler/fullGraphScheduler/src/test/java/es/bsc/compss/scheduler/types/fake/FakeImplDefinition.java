/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package es.bsc.compss.scheduler.types.fake;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.definition.ImplementationDefinition;


public class FakeImplDefinition extends ImplementationDefinition<FakeResourceDescription> {

    public FakeImplDefinition(String signature, FakeResourceDescription desc) {
        super(signature, desc);
    }

    @Override
    public Implementation getImpl(int coreId, int implId) {
        return new FakeImplementation(coreId, implId, this.getSignature(), this.getConstraints());
    }
}
