/*
 * Crail-terasort: An example terasort program for Sprak and crail
 *
 * Author: Animesh Trivedi <atr@zurich.ibm.com>
 *         Jonas Pfefferle <jpf@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.terasort;

import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;


import java.io.Serializable;
import java.util.Hashtable;

public class ParseTeraOptions implements Serializable {
    private Options options;
    private String testNames[];
    private int testIndex;
    private String serializer[];
    private int serializerIndex;
    private String inputDir;
    private String outputDir;
    private int bufferSize;
    private long paritionSize;
    private boolean isPartitionSet;
    private boolean syncOutput;
    private Hashtable<String,String> sparkParams;

    ParseTeraOptions(){
        options = new Options();
        sparkParams = new Hashtable<String,String>();

        testNames = new String[]{"loadonly", "loadstore", "loadcount", "loadcountstore", "loadsort", "loadsortstore"};
        testIndex = 5; /* loadsortstore is the default */

        serializer = new String[] {"none", "kryo", "byte", "f22"};
        serializerIndex = 0; /* default is none */

        inputDir = null;
        outputDir= null;
        bufferSize = 4096; //1048576;

        paritionSize = -1;
        isPartitionSet = false;
        syncOutput = false;

        options.addOption("h", "help", false, "show help.");
        options.addOption("n", "testname", true, "<string> Name of the test valid tests are :\n" +
                        "1. loadOnly: load and counts the input dataset\n" +
                        "2. loadStore: load the input dataset and stores it\n" +
                        "3. loadCount: load, shuffle, and then count the \n" +
                        "   resulting dataset\n" +
                        "4. loadCountStore: load, shuffle, count, and then \n" +
                        "   store the resulting dataset\n" +
                        "5. loadSort: load, shuffle, and then sort on key \n" +
                        "   the resulting dataset\n" +
                        "6. loadSortStore: load, shuffle, sort on key, then \n" +
                        "   store the resulting dataset\n"+
                        "the default is : loadSortStore");
        options.addOption("i", "inputDir", true, "<string> Name of the input directory");
        options.addOption("o", "outputDir", true, "<string> Name of the output directory");
        options.addOption("S", "sync", true, "<int> Takes 0 or 1 to pass to the sync call to the output \n" +
                "FS while writing (default: 0)");
        options.addOption("p", "partitionSize", true, "<long> Partition size, takes k,m,g,t suffixes\n" +
                "(default: input partition size, HDFS 2.6 has 128MB)");
        options.addOption("s", "useSerializer", true, "<string> You can use following serializers: \n" +
                "none: uses the Spark default serializer \n" +
                 "kryo: optimized Kryo for TeraSort \n" +
                 "byte: a simple byte[] serializer \n" +
                 "f22: an optimized crail-specific byte[] serializer\n" +
                 "     f22 requires CrailShuffleNativeRadixSorter for sorting\n");
        options.addOption("O", "options", true, "string,string : Sets properties on the Spark context. The first \n" +
                "string is the key, and the second is the value");
        options.addOption("b", "bufferSize", true, "<int> Buffer size for Kryo (only valid for kryo)");
        //options.addOption("K", "useKryoOptimizations", true, "<int> use kryoOptimizations (NYI)");
    }

    public String showOptions() {
        String str="\n";
        str+= "testName      : " + testNames[testIndex] + " \n";
        str+= "inputDir      : " + inputDir + "\n";
        str+= "outputDir     : " + outputDir + "\n";
        str+= "bufferSize    : " + bufferSize + "\n";
        str+= "serializer    : " + serializer[serializerIndex] + "\n";
        str+= "partitionSize : " + ((isPartitionSet)?(paritionSize):("sizeNotSet, using the default from HDFS")) + "\n";
        str+= "sync output   : " + syncOutput + "\n";
        str+= "spark options : ";
        if(sparkParams.size() == 0)
            str+=" none " + " \n";
        else {
            str+="\n";
            for (String key : sparkParams.keySet()) {
                str += "                 key: " + key + " value: " + sparkParams.get(key) + " \n";
            }
        }
        str+="\n";
        return str;
    }


    private int getMatchingIndex(String[] options, String name) {
        int i;
        for(i = 0; i < options.length; i++)
        if (name.equalsIgnoreCase(options[i])){
            return i;
        }
        throw  new IllegalArgumentException(name + " not found in " + options);
    }

    public void show_help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
    }

    public boolean getSyncOutput(){
        return syncOutput;
    }

    public int getBufferSize(){
        return bufferSize;
    }

    public String getInputDir(){
        return inputDir;
    }

    public String getOutputDir(){
        return outputDir;
    }

    public long getParitionSize(){
        return paritionSize;
    }

    public boolean isPartitionSet(){
        return isPartitionSet;
    }

    public boolean isTestLoadOnly(){
        return testIndex == 0;
    }

    public boolean isTestLoadStore(){
        return testIndex == 1;
    }

    public boolean isTestLoadCount(){
        return testIndex == 2;
    }

    public boolean isTestLoadCountStore(){
        return testIndex == 3;
    }
    public boolean isTestLoadSort(){
        return testIndex == 4;
    }
    public boolean isTestLoadSortStore(){
        return testIndex == 5;
    }

    public boolean isSerializerKryo(){
        return serializerIndex == 1;
    }

    public boolean isSerializerByte(){
        return serializerIndex == 2;
    }

    public boolean isSerializerF22(){
        return serializerIndex == 3;
    }

    public void setSparkOptions(SparkConf conf){
        for (String key : sparkParams.keySet()){
            System.err.println(" Setting up key: "+ key + " value: " + sparkParams.get(key));
            conf.set(key, sparkParams.get(key));
        }
    }

    public long sizeStrToBytes(String str) {
        String lower = str.toLowerCase();
        long val;
        if (lower.endsWith("k")) {
            val = Long.parseLong(lower.substring(0, lower.length() - 1)) * 1000;
        } else if (lower.endsWith("m")) {
            val = Long.parseLong(lower.substring(0, lower.length() - 1)) * 1000 * 1000;
        } else if (lower.endsWith("g")) {
            val = Long.parseLong(lower.substring(0, lower.length() - 1)) * 1000 * 1000 * 1000;
        } else if (lower.endsWith("t")) {
            val = Long.parseLong(lower.substring(0, lower.length() - 1)) * 1000 * 1000 * 1000 * 1000;
        } else {
            // no suffix, so it's just a number in bytes
            val = Long.parseLong(lower);
        }
        return val;
    }

    public void parse(String[] args) {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        int ioset = 0;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }
            if (cmd.hasOption("n")) {
                this.testIndex = getMatchingIndex(testNames,
                        cmd.getOptionValue("n").trim());
            }
            if (cmd.hasOption("i")) {
                inputDir = cmd.getOptionValue("i");
                ioset++;
            }
            if (cmd.hasOption("o")) {
                outputDir = cmd.getOptionValue("o");
                ioset++;
            }
            if (cmd.hasOption("s")) {
                this.serializerIndex = getMatchingIndex(serializer,
                        cmd.getOptionValue("s").trim());
            }
            if (cmd.hasOption("b")) {
                bufferSize = Integer.parseInt((cmd.getOptionValue("b")));
            }
            if (cmd.hasOption("p")) {
                paritionSize = sizeStrToBytes(cmd.getOptionValue("p"));
                isPartitionSet = true;
            }
            if (cmd.hasOption("S")) {
                if(Integer.parseInt(cmd.getOptionValue("S")) == 0)
                    syncOutput = false;
                else
                    syncOutput = true;
            }
            if(cmd.hasOption("O")) {
                String[] vals = cmd.getOptionValue("O").split(",");
                if(vals.length !=2) {
                    System.err.println("Failed to parse " + cmd.getOptionValue("O"));
                    System.exit(-1);
                }
                /* otherwise we got stuff */
                sparkParams.put(vals[0].trim(), vals[1].trim());
            }
        } catch (ParseException e) {
            System.err.println("Failed to parse command line properties" + e);
            show_help();
            System.exit(-1);
        }
        if(ioset != 2) {
            System.err.println(" Please set input and output directories atleast ! ");
            show_help();
            System.exit(-1);
        }
    }
}
