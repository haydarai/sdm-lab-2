package utils;

import java.io.File;

public class FileUtil {
    public static File loadFile(String fileName) {
        ClassLoader classLoader = FileUtil.class.getClassLoader();
        return new File(classLoader.getResource(fileName).getFile());
    }
}
