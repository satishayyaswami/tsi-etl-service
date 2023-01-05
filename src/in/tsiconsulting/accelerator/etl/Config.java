package in.tsiconsulting.accelerator.etl;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.util.Scanner;

public class Config {
    JSONObject config = null;
    String confDir = null;

    public Config(String confDir) throws Exception{
        this.confDir = confDir;
        this.config = readConfig(confDir);
    }

    public JSONObject getConfig(){
        return config;
    }

    public String getConfDir(){ return confDir;}

    private JSONObject readConfig(String confDir) throws Exception{
        JSONObject config = null;
        StringBuffer buff = new StringBuffer();

        File file = new File(confDir+"/"+"config.json");
        Scanner sc = new Scanner(file);

        while (sc.hasNextLine())
            buff.append(sc.nextLine());

        config = (JSONObject) new JSONParser().parse(buff.toString());
        return config;
    }
}
