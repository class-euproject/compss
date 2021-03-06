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
#ifndef GS_COMPSS_H
#define GS_COMPSS_H


#include "AbstractCache.h"
#include "common.h"

void GS_On(AbstractCache *);

/*** ==============> API FUNCTIONS <================= ***/

// COMPSs Runtime state
extern "C" void GS_On(void);
extern "C" void GS_Off(int code);
extern "C" void GS_Cancel_Application_Tasks(long appId);

// Task methods
extern "C" void GS_RegisterCE(char *CESignature,
                              char *ImplSignature,
                              char *ImplConstraints,
                              char *ImplType,
                              int num_params,
                              char **ImplTypeArgs
                             );
extern "C" void GS_ExecuteTask(long appId,
                               char *class_name,
                               char *on_failure,
                               int time_out,
                               char *method_name,
                               int priority,
                               int has_target,
                               int num_returns,
			                   int num_params,
                               void **params
                              );
extern "C" void GS_ExecuteTaskNew(long appId,
                                  char *signature,
                                  char *on_failure,
                                  int time_out,
                                  int priority,
                                  int num_nodes,
                                  int replicated,
                                  int distributed,
                                  int has_target,
                                  int num_returns,
                                  int num_params,
                                  void **params
                                 );

// File methods
extern "C" void GS_Open_File(char *file_name, int mode, char **buf);
extern "C" void GS_Close_File(char *file_name, int mode);
extern "C" void GS_Delete_File(char *file_name, int waitForData);
extern "C" void GS_Get_File(long appId, char *file_name);

// COMPSs API Calls
extern "C" void GS_Barrier(long appId);
extern "C" void GS_BarrierNew(long appId, int noMoreTasks);
extern "C" void GS_BarrierGroup(long _appId, char *group_name, char **exception_message);
extern "C" void GS_OpenTaskGroup(char *group_name, int implicitBarrier, long appId);
extern "C" void GS_CloseTaskGroup(char *group_name, long appId);

// Misc functions
extern "C" void GS_Get_AppDir(char **buf);
extern "C" void GS_EmitEvent(int type, long id);
extern "C" void GS_Get_Object(char *objectId, char**buf);
extern "C" void GS_Delete_Object(char *objectId, int **buf);

#endif /* GS_COMPSS_H */
