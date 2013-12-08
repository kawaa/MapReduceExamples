/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hakunamapdata.examples;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author kawaa
 */
public class Utils {
    
    public static final String COMMA = ",";

    public static void enableProfiling(Configuration conf, String maps, String reduces) {
        conf.setBoolean("mapred.task.profile", true);
        conf.set("mapred.task.profile.params", "-agentlib:hprof=cpu=samples,"
                + "heap=sites,depth=8,force=n,thread=y,verbose=n,file=%s");
        conf.set("mapred.task.profile.maps", maps);
        conf.set("mapred.task.profile.reduces", reduces);
    }
}
