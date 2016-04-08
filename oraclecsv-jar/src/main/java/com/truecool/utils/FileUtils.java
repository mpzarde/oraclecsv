package com.truecool.utils;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: mpzarde
 * Date: Apr 26, 2006
 * Time: 10:35:21 AM
 */
public class FileUtils {

  public static boolean fileExists(String fileName) {
    File file = new File(fileName);
    return file.exists();
  }

  public static boolean moveFiles(String srcDirPath, String destDirPath, String oldPattern, String newPattern) {
    boolean returnValue = true;

    File srcDir = new File(srcDirPath);
    if (!srcDir.isDirectory()) {
      return false;
    }

    File fileList[] = srcDir.listFiles();
    if (fileList != null) {
      long iNumCount_root = srcDir.list().length;
      for (int index = 0; (long) index < iNumCount_root; index++) {
        File currentFile = fileList[index];
        if (!currentFile.isFile() || currentFile.getName().indexOf(oldPattern) <= 0) {
          continue;
        }
        String fileName = currentFile.getName();
        fileName = StringUtils.replace(fileName, oldPattern, newPattern, false);
        String newFileName = String.valueOf(String.valueOf((new StringBuffer(String.valueOf(String.valueOf(destDirPath)))).append("/").append(fileName)));
        File newFile = new File(newFileName);
        if (newFile.exists()) {
          newFile.delete();
        }
        returnValue = currentFile.renameTo(newFile);
      }
    }

    return returnValue;
  }

  public static boolean deleteFile(String filePath) {
    File srcFile = new File(filePath);

    if (srcFile.isDirectory()) {
      File fileList[] = srcFile.listFiles();
      if (fileList != null) {
        long iNumCount_root = srcFile.list().length;
        for (int index = 0; (long) index < iNumCount_root; index++) {
          File currentFile = fileList[index];
          currentFile.delete();
        }
      }
    }

    return srcFile.delete();
  }

}
