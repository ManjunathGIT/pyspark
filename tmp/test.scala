import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

object HelloWorld {
    def main(args: Array[String]): Unit = {
    	var reader: BufferedReader = null

    	try {
    		Configuration conf = new Configuration()

    		var fs = FileSystem.get(URI.create("hdfs://dip.cdh5.dev:8020/user/yurun/text/text1"), conf);

    		reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))))

    		var line = reader.readLine

    		while (line != null) {
    			println(line)

    			line = reader.readLine
    		}
		} finally {	
    		if( reader != null) {
    			reader.close
    		}
		}
    }

}