package core;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  final private static Logger LOG =
      LoggerFactory.getLogger(Utils.class.getName());

  public static int computeAttrId(String sourceName, String columnName) {
    String t = sourceName.concat(columnName);
    return t.hashCode();
  }

  public static void appendLineToFile(File errorLogFile, String msg) {
    try {
      PrintWriter out = new PrintWriter(
          new BufferedWriter(new FileWriter(errorLogFile, true)));
      out.println(msg);
      out.close();
    } catch (IOException io) {
      io.printStackTrace();
      LOG.warn("Error log could not be written to error log file");
    }
  }
}
