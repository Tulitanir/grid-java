package org.grid.distributor;

import org.grid.annotations.Data;
import org.grid.annotations.Entrypoint;
import org.grid.annotations.Result;
import org.grid.annotations.SubtaskCount;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class Task {
    private String taskFolder;
    private BlockingQueue<Subtask> results;
    private final List<String> fileNames;

    public Task(String taskFolder) {
        this.taskFolder = taskFolder;
        this.fileNames = new ArrayList<>();
        validateTask();
    }

    private void validateTask() throws RuntimeException {
        if (taskFolder == null)
            throw new RuntimeException("unknown task folder");
        File jarFile = Arrays.stream(Objects.requireNonNull(new File(taskFolder).listFiles()))
                .filter(f -> f.getName().endsWith(".jar"))
                .findFirst().orElseThrow();

        fileNames.add(jarFile.getName());

        Long subtaskCount;
        boolean hasResult = false;

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
                        Class<?> coordClass = classLoader.loadClass("org.grid.task.Coord");
                        Object sampleCoord = coordClass.getConstructor(int.class, int.class).newInstance(0, 0);
                        System.out.println("Sample Coord toString() via reflection: " + sampleCoord.toString());
                        Method hashCodeMethod = coordClass.getDeclaredMethod("hashCode");
                        System.out.println("hashCode method present: " + (hashCodeMethod.getDeclaringClass() == coordClass));

                        if (clazz.isAnnotationPresent(Result.class)) {
                            hasResult = true;
                        }

                        if (clazz.isAnnotationPresent(Entrypoint.class)) {
                            subtaskCount = checkEntrypoint(clazz);
                            if (subtaskCount == null) {
                                throw new RuntimeException("Can't determine subtask count");
                            }
                            results = new ArrayBlockingQueue<>(Math.toIntExact(subtaskCount));
                        }
                    }
                }
                if (!hasResult)
                    throw new RuntimeException("Can't find result type");
            } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
                     IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private Long checkEntrypoint(Class<?> workerClass) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        Long subtaskCount = null;
        Method entrypoint = null;
        for (Method method : workerClass.getDeclaredMethods()) {
            var parameterTypes = method.getParameterTypes();
            if (method.isAnnotationPresent(Entrypoint.class) && parameterTypes.length == 1 && (parameterTypes[0] == long.class ||  parameterTypes[0] == int.class)) {
                entrypoint = method;
            }

            if (method.isAnnotationPresent(SubtaskCount.class)) {
                List<Object> args = new ArrayList<>();

                var constructor = findDataConstructor(workerClass);
                for (Parameter param : constructor.getParameters()) {
                    if (param.isAnnotationPresent(Data.class)) {
                        String fileName = param.getAnnotation(Data.class).value();
                        Path filePath = Paths.get(taskFolder, fileName);
                        if (!Files.exists(filePath)) {
                            throw new RuntimeException(
                                    "Required data file not found: " + filePath);
                        }
                        fileNames.add(fileName);
                        var fileConstructor = param.getType().getConstructor(String.class);
                        args.add(fileConstructor.newInstance(filePath.toString()));
                    }
                }

                var obj = constructor.newInstance(args.toArray());
                subtaskCount = (Long) method.invoke(obj, null);
            }
        }
        if (entrypoint == null)
            throw new RuntimeException("Entrypoint method not found");

        return subtaskCount;
    }

    private Constructor<?> findDataConstructor(Class<?> workerClass) throws NoSuchMethodException {
        for (Constructor<?> constructor : workerClass.getDeclaredConstructors()) {
            boolean hasDataParams = Arrays.stream(constructor.getParameters())
                    .anyMatch(p -> p.isAnnotationPresent(Data.class));

            if (hasDataParams) {
                return constructor;
            }
        }
        throw new NoSuchMethodException("No constructor with @Data parameters found in " + workerClass.getName());
    }

    public String[] getFileNames() {
        return fileNames.toArray(new String[0]);
    }

    public void addResult(Subtask subtask) {
        results.add(subtask);
    }
}
