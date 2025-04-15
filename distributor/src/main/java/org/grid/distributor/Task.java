package org.grid.distributor;

import com.google.protobuf.ByteString;
import org.grid.annotations.*;

import java.io.*;
import java.lang.reflect.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Logger;

public class Task {
    private static final Logger logger = Logger.getLogger(Task.class.getName());

    private final Long id;
    private Iterator<?> subtaskIterator = null;
    private final String taskFolder;
    private ConcurrentMap<Long, Subtask> results;
    private final List<String> fileNames;
    private final ClassLoader classLoader;
    private final Class<?> resultType;
    private String jarFilePath = null;
    private Method aggregationMethod = null;
    private Object finalResult = null;

    private static class ValidationResult {
        ClassLoader classLoader = null;
        Iterator<?> iterator = null;
        Class<?> resultType = null;
        List<String> requiredFiles = new ArrayList<>();
        Class<?> entrypointInputType = null;
    }

    public String[] getFileNames() {
        return fileNames.toArray(new String[0]);
    }

    public ConcurrentMap<Long, Subtask> getResults() {
        return results;
    }

    public Iterator<?> getSubtaskIterator() {
        return subtaskIterator;
    }

    public Long getId() {
        return id;
    }

    public Task(Long id, String taskFolder) {
        this.id = id;
        this.taskFolder = taskFolder;
        this.fileNames = new ArrayList<>();
        this.results = new ConcurrentHashMap<>();
        ValidationResult validationResult = validateTask();
        this.classLoader = validationResult.classLoader;
        this.subtaskIterator = validationResult.iterator;
        this.resultType = validationResult.resultType;
        this.fileNames.addAll(validationResult.requiredFiles);
    }

    public URLClassLoader getTaskClassLoader() {
        try {
            return new URLClassLoader(
                    new URL[]{new File(jarFilePath).toURI().toURL()},
                    Distributor.class.getClassLoader()
            );
        } catch (MalformedURLException e) {
            throw new RuntimeException("Failed to create URL for task JAR: " + jarFilePath, e);
        }
    }

    private ValidationResult validateTask() throws RuntimeException {
        if (taskFolder == null)
            throw new RuntimeException("unknown task folder");
        File jarFile = Arrays.stream(Objects.requireNonNull(new File(taskFolder).listFiles()))
                .filter(f -> f.getName().endsWith(".jar"))
                .findFirst().orElseThrow();

        fileNames.add(jarFile.getName());
        jarFilePath = jarFile.getAbsolutePath();

        boolean entrypointClassFound = false;
        boolean iteratorMethodFound = false;
        boolean resultClassFound = false;
        Object taskInstance = null;

        ValidationResult validationResult = new ValidationResult();

        try (URLClassLoader classLoader = new URLClassLoader(
                new URL[]{jarFile.toURI().toURL()},
                getClass().getClassLoader())) {
            try (JarFile jar = new JarFile(jarFile)) {
                Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    if (entry.getName().endsWith(".class") && !entry.getName().endsWith("module-info.class")) {
                        String className = entry.getName()
                                .replace("/", ".")
                                .replace(".class", "");
                        Class<?> clazz = classLoader.loadClass(className);

                        if (clazz.isAnnotationPresent(Result.class)) {
                            if (!Serializable.class.isAssignableFrom(clazz))
                                throw new RuntimeException("@Result class " + clazz.getName() + " is not Serializable");
                            resultClassFound = true;
                            validationResult.resultType = clazz;
                            logger.fine("Found result class: " + clazz.getName());
                        }

                        if (clazz.isAnnotationPresent(Entrypoint.class)) {
                            if (entrypointClassFound)
                                throw new RuntimeException("Multiple classes annotated with @Entrypoint found in " + jarFile.getName());
                            entrypointClassFound = true;
                            logger.fine("Found @Entrypoint class: " + className);

                            Constructor<?> dataConstructor = findDataConstructor(clazz, validationResult.requiredFiles);
                            taskInstance = instantiateForData(dataConstructor);

                            Method iteratorMethod = findSubtaskIteratorMethod(clazz);
                            if (iteratorMethod != null) {
                                iteratorMethod.setAccessible(true);
                                Object iteratorObj = iteratorMethod.invoke(taskInstance);
                                if (iteratorObj instanceof Iterator) {
                                    validationResult.iterator = (Iterator<?>) iteratorObj;
                                    iteratorMethodFound = true;
                                    logger.fine("Found and invoked @SubtaskIterator method: " + iteratorMethod.getName());
                                } else {
                                    throw new RuntimeException("@SubtaskIterator method " + iteratorMethod.getName() + " did not return an Iterator. Returned: " + (iteratorObj != null ? iteratorObj.getClass().getName() : "null"));
                                }
                            }

                            Method entrypointMethod = findEntrypointMethod(clazz);
                            if (entrypointMethod != null) {
                                if (entrypointMethod.getParameterCount() != 1)
                                    throw new RuntimeException("@Entrypoint method " + entrypointMethod.getName() + " must accept exactly one parameter.");
                                validationResult.entrypointInputType = entrypointMethod.getParameterTypes()[0];
                                logger.fine("Found @Entrypoint method: " + entrypointMethod.getName() + " accepting " + validationResult.entrypointInputType.getName());

                                Class<?> entrypointReturnType = entrypointMethod.getReturnType();
                                if (entrypointReturnType != void.class && !Serializable.class.isAssignableFrom(entrypointReturnType)) {
                                    throw new RuntimeException("@Entrypoint method " + entrypointMethod.getName() + " returns non-Serializable type: " + entrypointReturnType.getName());
                                }
                                if (resultClassFound && !validationResult.resultType.isAssignableFrom(entrypointReturnType)) {
                                    throw new RuntimeException("Entrypoint method " + entrypointMethod.getName() + " return type (" + entrypointReturnType.getName() + ") is not assignable to the @Result class (" + validationResult.resultType.getName() + ")");
                                }
                            } else {
                                throw new RuntimeException("No method annotated with @Entrypoint found within the @Entrypoint class: " + className);
                            }

                            this.aggregationMethod = findAggregationMethod(clazz);
                            if (this.aggregationMethod == null) {
                                throw new RuntimeException("No method annotated with @Entrypoint found within the @Entrypoint class: " + className);
                            }
                        }
                    }
                }
                if (!entrypointClassFound) {
                    throw new RuntimeException("Can't find entrypoint class");
                }
                if (!resultClassFound)
                    throw new RuntimeException("Can't find result type");
                if (!iteratorMethodFound) {
                    throw new RuntimeException("Can't find subtask iterator");
                }

                validationResult.classLoader = classLoader;
            } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException |
                     IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return validationResult;
    }

    private Constructor<?> findDataConstructor(Class<?> workerClass, List<String> requiredFiles) throws NoSuchMethodException {
        Constructor<?> foundConstructor = null;
        for (Constructor<?> constructor : workerClass.getDeclaredConstructors()) {
            boolean hasDataParams = false;
            boolean hasNonDataParams = false;
            List<String> constructorFiles = new ArrayList<>();
            for (Parameter p : constructor.getParameters()) {
                if (p.isAnnotationPresent(Data.class)) {
                    hasDataParams = true;
                    String fileName = p.getAnnotation(Data.class).value();
                    if (fileName == null || fileName.trim().isEmpty())
                        throw new NoSuchMethodException("@Data has empty value.");
                    Path filePath = Paths.get(taskFolder, fileName);
                    if (!Files.exists(filePath))
                        throw new NoSuchMethodException("Data file '" + fileName + "' not found.");
                    try {
                        p.getType().getConstructor(String.class);
                    } catch (NoSuchMethodException e) {
                        throw new NoSuchMethodException("Data parameter '" + p.getName() + "' type " + p.getType().getName() + " has no String constructor.");
                    }
                    constructorFiles.add(fileName);
                } else {
                    hasNonDataParams = true;
                }
            }
            if (hasDataParams && !hasNonDataParams) {
                requiredFiles.addAll(constructorFiles);
                return constructor;
            }
            if (hasDataParams && foundConstructor == null) {
                foundConstructor = constructor;
                requiredFiles.addAll(constructorFiles);
            }
        }
        if (foundConstructor != null) {
            logger.warning("Using constructor with potentially non-@Data parameters in " + workerClass.getName());
            return foundConstructor;
        }
        try {
            return workerClass.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new NoSuchMethodException("No suitable constructor found in " + workerClass.getName());
        }
    }

    private Object instantiateForData(Constructor<?> constructor) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        List<Object> args = new ArrayList<>();
        for (Parameter param : constructor.getParameters()) {
            if (param.isAnnotationPresent(Data.class)) {
                String fileName = param.getAnnotation(Data.class).value();
                Path filePath = Paths.get(taskFolder, fileName);
                Constructor<?> fileParamConstructor = param.getType().getConstructor(String.class);
                args.add(fileParamConstructor.newInstance(filePath.toString()));
            } else {
                throw new InstantiationException("Cannot instantiate: Constructor parameter '" + param.getName() + "' is not annotated with @Data.");
            }
        }
        constructor.setAccessible(true);
        return constructor.newInstance(args.toArray());
    }

    private Method findEntrypointMethod(Class<?> workerClass) {
        for (Method method : workerClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(Entrypoint.class) && method.getParameterCount() == 1) {
                return method;
            }
        }
        return null;
    }

    private Method findSubtaskIteratorMethod(Class<?> workerClass) {
        for (Method method : workerClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(SubtaskIterator.class) &&
                    method.getParameterCount() == 0 &&
                    Iterator.class.isAssignableFrom(method.getReturnType())) {
                return method;
            }
        }
        return null;
    }

    private Method findAggregationMethod(Class<?> workerClass) {
        for (Method method : workerClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(Aggregator.class) &&
                    Modifier.isStatic(Modifier.methodModifiers()) &&
                    method.getParameterCount() == 2 &&
                    method.getReturnType() == method.getParameterTypes()[0] &&
                    method.getReturnType() == method.getParameterTypes()[1]
            ) {
                return method;
            }
        }
        return null;
    }

    public Object getFinalResult() {
        return finalResult;
    }

    public void addResult(Subtask subtask) throws IOException, ClassNotFoundException {
        Subtask old = results.get(subtask.getId());
        if (old == null || !old.getStatus().equals(SubtaskStatus.DONE)) {
            results.put(subtask.getId(), subtask);
        }

        var result = deserialize(subtask.getResult(), classLoader);

        if (result == null) {
            logger.warning("Result of subtask " + subtask.getId() + " is null");
            return;
        }
        if (!(this.resultType.isInstance(result))) {
            logger.warning("Result of subtask " + subtask.getId() + "  isn't instance of result type. " + result.getClass().getName() + ":" + this.resultType.getName());
        }

        try {
            if (this.finalResult == null) {
                this.finalResult = result;
            } else {
                this.finalResult = aggregationMethod.invoke(null, finalResult, result);
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object deserialize(ByteString byteString, ClassLoader taskClassLoader) throws IOException, ClassNotFoundException {
        if (byteString == null || byteString.isEmpty()) {
            return null;
        }
        byte[] data = byteString.toByteArray();
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new TaskSpecificObjectInputStream(bis, taskClassLoader)) {
            return ois.readObject();
        }
    }

    public long getAverageSubtaskExecutionTime() {
        long totalTime = 0;
        int count = results.size();
        for (Subtask st : results.values()) {
            totalTime += st.getExecutionTime();
        }
        return count > 0 ? totalTime / count : 0;
    }
}

