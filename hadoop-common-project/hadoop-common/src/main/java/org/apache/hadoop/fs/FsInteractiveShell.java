/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
import jline.console.history.History;
import jline.console.history.PersistentHistory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Private
public class FsInteractiveShell extends FsShell {

  static final Log LOG = LogFactory.getLog(FsShell.class);

  private ConsoleReader reader;

  protected static FsInteractiveShell newShellInstance() { return new FsInteractiveShell(); }

  @Override
  public int run(String[] argv) throws Exception {
    init();
    int ret = 0;
    try {
      setupConsoleReader();
      String line;
      String prefix = "";
      String curPath = getFS().getHomeDirectory().toString();
      String curPrompt = curPath;

      while ((line = reader.readLine(curPrompt + "> ").trim()) != null) {
        if (!prefix.equals("")) {
          prefix += '\n';
        }
        if (line.startsWith("#")) {
          continue;
        } else if (line.isEmpty()) {
          continue;
        }
        String[] command = line.split("\\s+");
        String commandWithHyphen = "-" + command[0];
        if (commandFactory.getInstance(commandWithHyphen) != null) {
          command[0] = commandWithHyphen;
          ret = super.run(command);
        } else {
          System.err.println(command[0] + ": command not found");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ret; // TODO
  }

  @Override
  protected String getUsagePrefix() {
    return "";
  }

  @Override
  protected String makeUsageString(Command instance) {
    return instance.getName() + " " + instance.getUsage();
  }

  @Override
  protected void printAdditionalInfo(PrintStream out) { /* nothing to do */ }

  private Completer[] getCommandCompleter() {
    // StringCompleter matches against a pre-defined wordlist
    // We start with an empty wordlist and build it up
    List<String> candidateStrings = new ArrayList<>();
    for (String command : commandFactory.getNames()) {
      if (!command.equals("-shell")) {
        if (command.startsWith("-")) {
          command = command.substring(1);
        }
        candidateStrings.add(command);
      }
    }

    final StringsCompleter strCompleter = new StringsCompleter(candidateStrings);

    Completer customCompleter = new StringsCompleter() {
      @Override
      public int complete(String buffer, int cursor, List<CharSequence> clist) {

        int ret = 0;
        if (buffer == null) {
          return 0;
        }
        String currentPath;
        if (buffer.startsWith("/")) {
          currentPath = buffer.substring(0, buffer.lastIndexOf("/") + 1);
        } else {
          currentPath = "/"; // TODO
        }
        try {
          FileStatus[] statuses = getFS().listStatus(new Path(currentPath));
          List<String> pathList = new ArrayList<>();

          for (FileStatus status : statuses) {
            String postFix= status.isDirectory() ? "/" : "";
            pathList.add(status.getPath().toUri().getPath() + postFix);
          }

          StringsCompleter pathCompleter = new StringsCompleter(pathList);
          pathCompleter.complete(buffer, cursor, clist);

          if (clist.size() == 1) {
            String p = (String)clist.get(0);
//            System.out.println(p);
//            System.out.println(getFS().isDirectory(new Path(p)));
//            if (getFS().getFileStatus(new Path(p)).isDirectory()) {
//            if (getFS().isDirectory(new Path(p))) {
//              clist.set(0, p.trim() + "/");
            clist.set(0, p.trim() );
//            } else {
  //            clist.set(0, p.trim());
//            }
          }

        } catch (IOException e) {
//          e.printStackTrace();
          // TODO
        }

        return ret;
      }
    };

    final ArgumentCompleter argCompleter = new ArgumentCompleter(strCompleter, customCompleter);

    return new Completer[] {argCompleter};
  }

  private void setupCmdHistory() {
    final String HISTORYFILE = ".dfsshellhistory";
    final String historyDir = System.getProperty("user.home");
    PersistentHistory history = null;

    try {
      if ((new File(historyDir)).exists()) {
          String historyFile = historyDir + File.separator + HISTORYFILE;
          history = new FileHistory(new File(historyFile));
          reader.setHistory(history);
      } else {
        System.err.println("WARNING: Directory for history file: " + historyDir +
            " does not exist. History will not be available during this session.");
      }
    } catch (Exception e) {
      System.err.println("WARNING: Encountered an error while trying to initialize history file." +
          " History will not be available during this session.");
      System.err.println(e.getMessage());
    }

    // add shutdown hook to flush the history to history file
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        History h = reader.getHistory();
          if (h instanceof FileHistory) {
            try {
              ((FileHistory)h).flush();
            } catch (IOException e) {
              System.err.println("WARNING: Failed to write command history file: " + e.getMessage());
            }
          }
      }
    }));
  }

  private void setupConsoleReader() throws IOException {
    reader = new ConsoleReader();
    reader.setExpandEvents(false);
    reader.setBellEnabled(false);
    for (Completer completer : getCommandCompleter()) {
      reader.addCompleter(completer);
    }
    setupCmdHistory();
  }


  /**
   * main() has some simple utility methods
   * @param argv the command and its arguments
   * @throws Exception upon error
   */
  public static void main(String argv[]) throws Exception {
    FsInteractiveShell shell = newShellInstance();
    Configuration conf = new Configuration();
    conf.setQuietMode(false);
    shell.setConf(conf);
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }
}