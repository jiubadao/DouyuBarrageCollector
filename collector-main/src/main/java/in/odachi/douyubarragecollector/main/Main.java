package in.odachi.douyubarragecollector.main;

import in.odachi.douyubarragecollector.master.main.Master;
import in.odachi.douyubarragecollector.slave.main.Slave;

/**
 * 启动类
 */
public class Main {

    public static void main(String[] args) {
        new Slave().start();
        new Master().start();
    }
}
