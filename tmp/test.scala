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
    		var file = new File("/Users/yurun/Downloads/yarn-site.xml")

    		reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))

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