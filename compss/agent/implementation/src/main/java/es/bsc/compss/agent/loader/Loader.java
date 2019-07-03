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
package es.bsc.compss.agent.loader;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.agent.AgentException;
import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.loader.LoaderConstants;
import es.bsc.compss.loader.LoaderUtils;
import es.bsc.compss.loader.total.ITAppEditor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CodeConverter;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.Modifier;
import javassist.NotFoundException;


public class Loader {

    /**
     * Instruments a method using a CEI and runs it passing in the parameters given.
     *
     * @param runtime COMPSs runtime that will handle the execution of the nested tasks
     * @param api COMPSs runtime that will handle the execution of the nested tasks
     * @param ceiClass CEI to detect tasks
     * @param appId application Id that will be used by the nested tasks
     * @param className name of the class containing the method to run
     * @param methodName name of the method to run
     * @param params values of the parameters to pass in to the method
     * @return returns the return value of the executed method
     * @throws AgentException could not instrument the code or the execution raised an exception
     */
    public static Object load(COMPSsRuntime runtime, LoaderAPI api, String ceiClass, long appId, String className,
            String methodName, ApplicationParameter... params) throws AgentException {
        try {
            // Add the jars that the custom class loader needs
            String compssHome = System.getenv(COMPSsConstants.COMPSS_HOME);
            ClassLoader myLoader = new URLClassLoader(
                    new URL[]{new URL("file:" + compssHome + LoaderConstants.ENGINE_JAR_WITH_REL_PATH)});

            Thread.currentThread().setContextClassLoader(myLoader);

            ClassPool classPool = getClassPool();
            CtClass appClass = classPool.get(className);
            appClass.defrost();

            String varName = LoaderUtils.randomName(5, LoaderConstants.STR_COMPSS_PREFIX);
            String itApiVar = varName + LoaderConstants.STR_COMPSS_API;
            String itSRVar = varName + LoaderConstants.STR_COMPSS_STREAM_REGISTRY;
            String itORVar = varName + LoaderConstants.STR_COMPSS_OBJECT_REGISTRY;
            String itAppIdVar = varName + LoaderConstants.STR_COMPSS_APP_ID;

            addVariables(classPool, appClass, itApiVar, itSRVar, itORVar, itAppIdVar);
            instrumentClass(classPool, appClass, ceiClass, itApiVar, itSRVar, itORVar, itAppIdVar);
            addModifyVariablesMethod(appClass, itApiVar, itSRVar, itORVar, itAppIdVar);

            Class<?> app = appClass.toClass();
            appClass.defrost();

            Method setter = app.getDeclaredMethod("setCOMPSsVariables",
                    new Class<?>[]{Class.forName(LoaderConstants.CLASS_COMPSSRUNTIME_API),
                        Class.forName(LoaderConstants.CLASS_LOADERAPI),
                        Class.forName(LoaderConstants.CLASS_APP_ID)});

            Object[] values = new Object[]{runtime, api, appId};
            setter.invoke(null, values);

            Object[] paramValues = new Object[params.length];
            Class<?>[] types = new Class<?>[params.length];
            for (int i = 0; i < params.length; i++) {
                paramValues[i] = params[i].getValueContent();
                types[i] = paramValues[i].getClass();
            }

            Method main = findMethod(app, methodName, params.length, types, params);
            Object returnValue = main.invoke(null, params);
            runtime.noMoreTasks(appId);
            return returnValue;

        } catch (Throwable e) {
            throw new AgentException(e);
        }
    }

    private static void addVariables(ClassPool cp, CtClass appClass, String itApiVar, String itSRVar, String itORVar,
            String itAppIdVar) throws NotFoundException, CannotCompileException {
        CtClass itApiClass = cp.get(LoaderConstants.CLASS_COMPSSRUNTIME_API);
        CtField itApiField = new CtField(itApiClass, itApiVar, appClass);
        itApiField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        appClass.addField(itApiField);

        CtClass itSRClass = cp.get(LoaderConstants.CLASS_STREAM_REGISTRY);
        CtField itSRField = new CtField(itSRClass, itSRVar, appClass);
        itSRField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        appClass.addField(itSRField);

        CtClass itORClass = cp.get(LoaderConstants.CLASS_OBJECT_REGISTRY);
        CtField itORField = new CtField(itORClass, itORVar, appClass);
        itORField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        appClass.addField(itORField);

        CtClass appIdClass = cp.get(LoaderConstants.CLASS_APP_ID);
        CtField appIdField = new CtField(appIdClass, itAppIdVar, appClass);
        appIdField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        appClass.addField(appIdField);
    }

    private static void instrumentClass(ClassPool cp, CtClass appClass, String ceiClass, String itApiVar,
            String itSRVar, String itORVar, String itAppIdVar)
            throws ClassNotFoundException, NotFoundException, CannotCompileException {
        Class<?> annotItf = Class.forName(ceiClass);
        Method[] remoteMethods = annotItf.getMethods();

        CtMethod[] instrCandidates = appClass.getDeclaredMethods();
        // Candidates to be instrumented if they are not remote
        ITAppEditor itAppEditor;
        itAppEditor = new ITAppEditor(remoteMethods, instrCandidates, itApiVar, itSRVar, itORVar, itAppIdVar, appClass);

        /*
         * Create Code Converter
         */
        CodeConverter converter = new CodeConverter();
        CtClass arrayWatcher = cp.get(LoaderConstants.CLASS_ARRAY_ACCESS_WATCHER);
        CodeConverter.DefaultArrayAccessReplacementMethodNames names;
        names = new CodeConverter.DefaultArrayAccessReplacementMethodNames();
        converter.replaceArrayAccess(arrayWatcher, (CodeConverter.ArrayAccessReplacementMethodNames) names);

        /*
         * Find the methods declared in the application class that will be instrumented - Main - Constructors - Methods
         * that are not in the remote list
         */
        for (CtMethod m : instrCandidates) {
            m.instrument(converter);
            m.instrument(itAppEditor);
        }

        // Instrument constructors
        for (CtConstructor c : appClass.getDeclaredConstructors()) {
            c.instrument(converter);
            c.instrument(itAppEditor);
        }
    }

    private static void addModifyVariablesMethod(CtClass appClass, String itApiVar, String itSRVar, String itORVar,
            String itAppIdVar) throws CannotCompileException, NotFoundException {

        String methodBody;
        CtMethod m;
        methodBody = "public static void printCOMPSsVariables() { "
                + "System.out.println(\"Api Var: \" + " + itApiVar + ");"
                + "System.out.println(\"SR Var: \" + " + itSRVar + ");"
                + "System.out.println(\"OR Var: \" + " + itORVar + ");"
                + "System.out.println(\"App Id: \" + " + itAppIdVar + ");"
                + "}";
        m = CtNewMethod.make(methodBody, appClass);
        appClass.addMethod(m);

        methodBody = "public static void setCOMPSsVariables( "
                + LoaderConstants.CLASS_COMPSSRUNTIME_API + " runtime" + ", "
                + LoaderConstants.CLASS_LOADERAPI + " loader" + ", "
                + LoaderConstants.CLASS_APP_ID + " appId" + ") { \n"
                + itApiVar + "= runtime; \n"
                + itSRVar + "= loader.getStreamRegistry(); \n"
                + itORVar + "= loader.getObjectRegistry(); \n"
                + itAppIdVar + "= appId; \n"
                + "}";
        m = CtNewMethod.make(methodBody, appClass);
        appClass.addMethod(m);
    }

    private static ClassPool getClassPool() {
        ClassPool cp = new ClassPool();
        cp.appendSystemPath();
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_ROOT);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_API);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_API_IMPL);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_LOADER);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_LOADER_TOTAL);
        return cp;
    }

    private static Method findMethod(Class<?> methodClass, String methodName, int numParams, Class<?>[] types,
            Object[] values) {
        Method method = null;
        try {
            method = methodClass.getMethod(methodName, types);
        } catch (NoSuchMethodException | SecurityException e) {
            for (Method m : methodClass.getDeclaredMethods()) {
                if (m.getName().equals(methodName) && numParams == m.getParameterCount()) {
                    int paramId = 0;
                    boolean isMatch = true;
                    for (java.lang.reflect.Parameter p : m.getParameters()) {
                        Class<?> methodParamClass = p.getType();
                        if (methodParamClass.isPrimitive()) {
                            Class<?> taskParamClass = values[paramId].getClass();
                            if (methodParamClass != taskParamClass) {
                                isMatch &= isBasicTypeMatch(methodParamClass, taskParamClass);
                            }
                        } else {
                            try {
                                methodParamClass.cast(values[paramId]);
                            } catch (ClassCastException cce) {
                                isMatch = false;
                                break;
                            }
                        }
                        paramId++;
                    }
                    if (isMatch) {
                        method = m;
                    }
                }
            }
        }
        return method;
    }

    private static boolean isBasicTypeMatch(Class<?> methodParamClass, Class<?> taskParamClass) {
        String methodParamClassName = methodParamClass.getCanonicalName();
        String taskParamClassName = taskParamClass.getCanonicalName();
        boolean isMatch = false;
        switch (methodParamClassName) {
            case "byte":
                isMatch = taskParamClassName.equals("java.lang.Byte");
                break;
            case "char":
                isMatch = taskParamClassName.equals("java.lang.Char");
                break;
            case "short":
                isMatch = taskParamClassName.equals("java.lang.Short");
                break;
            case "int":
                isMatch = taskParamClassName.equals("java.lang.Integer");
                break;
            case "long":
                isMatch = taskParamClassName.equals("java.lang.Long");
                break;
            case "float":
                isMatch = taskParamClassName.equals("java.lang.Float");
                break;
            case "double":
                isMatch = taskParamClassName.equals("java.lang.Double");
                break;
            case "boolean":
                isMatch = taskParamClassName.equals("java.lang.Boolean");
                break;
        }
        return isMatch;
    }

    /**
     * Debug method to print the variables added by the instrumentation process.
     * Invokes by reflection the printCOMPSsVariables method of a given class.
     *
     * @param app instrumented app
     * @throws Exception Error while calling the method to print the variables
     */
    public static void printVariables(Class<?> app) throws Exception {
        Method setter = app.getDeclaredMethod("printCOMPSsVariables", new Class<?>[]{});
        Object[] values = new Object[]{};
        setter.invoke(null, values);
    }
}
