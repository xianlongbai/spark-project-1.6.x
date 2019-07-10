package zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


/**
 * 操作zookeeper 的  API
 * Created by root on 2018/4/4.
 *
 *GET 操作对应的属性含义
 *   czxid. 节点创建时的zxid.
     mzxid. 节点最新一次更新发生时的zxid.
     ctime. 节点创建时的时间戳.
     mtime. 节点最新一次更新发生时的时间戳.
     dataVersion. 节点数据的更新次数.
     cversion. 其子节点的更新次数.
     aclVersion. 节点ACL(授权信息)的更新次数.
     ephemeralOwner. 如果该节点为ephemeral节点, ephemeralOwner值表示与该节点绑定的session id. 如果该节点不是ephemeral节点, ephemeralOwner值为0. 至于什么是ephemeral节点, 请看后面的讲述.
     dataLength. 节点数据的字节数.
     numChildren. 子节点个数.
 *
 *zxid 是何意 有什么用？
 *   ZooKeeper状态的每一次改变, 都对应着一个递增的Transaction id, 该id称为zxid. 由于zxid的递增性质, 如果zxid1小于zxid2,
 *   那么zxid1肯定先于zxid2发生. 创建任意节点, 或者更新任意节点的数据, 或者删除任意节点, 都会导致Zookeeper状态发生改变,
 *   从而导致zxid的值增加.
 *
 *
 *
 *
 */

public class TestZook implements Watcher {

    private static ZooKeeper zooKeeper = null;
    /**
     * 状态信息, 描述该znode的版本, 权限等信息.
     */
     private static Stat stat = new Stat();

    /**
     * ZooKeeper(String connectString, int sessionTimeout, Watcher watcher)
     * ZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly)
     *      connectString：以逗号分隔的主机:端口号列表，每个对应一个ZooKeeper服务器。 例如，10.0.0.1:2001，10.0.0.2:2002和10.0.0.3:2003表示三个节点的ZooKeeper ensemble的有效的主机:端口匹配对。
     *      sessionTimeout：这是以毫秒为单位的会话超时时间。这是ZooKeeper在宣布session结束之前，没有从客户端获得心跳的时间。
     *      watcher：一个watcher对象，如果创建，当状态改变和发生节点事件时会收到通知。这个watcher对象需要通过一个用户定义的类单独创建，通过实现Watcher接口并将实例化的对象传递给ZooKeeper构造方法。客户端应用程序可以收到各种类型的事件的通知，例如连接丢失、会话过期等。
     *      canBeReadOnly:如果为true canBeReadOnly参数允许创建的客户端在网络分区的情况下进入只读模式。只读模式是客户端无法找到任何多数服务器的场景，但有一个可以到达的分区服务器，以只读模式连接到它，这样就允许对服务器的读取请求，而写入请求则不允许
     *
     * ZooKeeper(String connectString, int sessionTimeout, Watcher watcher, long sessionId, byte[] sessionPasswd)
     *      sessionId：在客户端重新连接到ZooKeeper服务器的情况下，可以使用特定的会话ID来引用先前连接的会话
            sessionPasswd：如果指定的会话需要密码，可以在这里指定
     *
     * ZooKeeper(String connectString, int sessionTimeout,Watcher watcher, long sessionId, byte[] sessionPasswd,boolean canBeReadOnly)
     *      此构造方法是前两个调用的组合，允许在启用只读模式的情况下重新连接到指定的会话。
     *
     *session
     *      在client和server通信之前, 首先需要建立连接, 该连接称为session. 连接建立后, 如果发生连接超时, 授权失败, 或者显式关闭连接, 连接便处于CLOSED状态, 此时session结束
     *
     */
    @Before
    //@Test
    public void connZook() throws IOException {
        zooKeeper = new ZooKeeper("node2:2181,node3:2181,node4:2181",3000,new TestZook());
    }

    /**
     * 连接zk时可触发的事件
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event:" + event);
        if(event.getState() == Event.KeeperState.SyncConnected){
            System.out.println("ZooKeeper session established.");
            if(Event.EventType.None == event.getType() && null == event.getPath() ){
                System.out.println(111);
            }else if(event.getType() == Event.EventType.NodeChildrenChanged){
                try {
                    //在process里面又注册了Watcher，否则，将无法获取node11节点的创建而导致子节点变化的事件。
                    System.out.println("ReGet Child:" + zooKeeper.getChildren(event.getPath(), true));
                    System.out.println(222);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (event.getType() == Event.EventType.NodeDataChanged) {
                try {
                    //在process里面又注册了Watcher，否则，将无法获取再次修改节点时而导致子节点变化的事件。
                    System.out.println("the data of znode " + event.getPath() + " is : " + new String(zooKeeper.getData(event.getPath(), true, stat)));
                    System.out.println("czxID: " + stat.getCzxid() + ", mzxID: " + stat.getMzxid() + ", version: " + stat.getVersion());
                    System.out.println(333);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            /** 在创建节点和删除节点时，判断节点是否已经存在 */
            }else if (Event.EventType.NodeCreated == event.getType()) {
                System.out.println("success create znode: " + event.getPath());
                try {
                    System.out.println(444);
                    zooKeeper.exists(event.getPath(), true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (Event.EventType.NodeDeleted == event.getType()) {
                System.out.println("success delete znode: " + event.getPath());
                try {
                    System.out.println(555);
                    zooKeeper.exists(event.getPath(), true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @After
    public void closeZook() throws InterruptedException {
        zooKeeper.close();
    }


    /**
     *
     * String create(String path, byte[] data, List acl, CreateMode createMode);
     *  --path:注意路径问题：必须以 "/" 开始
     *  --CreateMode:
     *       CreateMode.PERSISTENT 	永久性节点
             CreateMode.PERSISTENT_SEQUENTIAL 	永久性序列节点
             CreateMode.EPHEMERAL 	临时节点，会话断开或过期时会删除此节点
             CreateMode.PERSISTENT_SEQUENTIAL 	临时序列节点，会话断开或过期时会删除此节点
     *  --data:与znode关联的数据.
     *  --指定权限信息, 如果不想指定权限, 可以传入Ids.OPEN_ACL_UNSAFE.(一个ACL对象由schema:ID和Permissions组成。)
     *        READ: 允许获取该节点的值和列出子节点。
     *        WRITE: 允许设置该节点的值。
     *        CREATE: 允许创建子节点。
     *        DELETE: 可以删除子节点。
     *        ADMIN: 允许为该节点设置权限。
     *
     *异步方式：
     * zookeeper.create("/zk-test-ephemeral-", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,new IStringCallback(), "I am context. ");
     *
     *
     */
    @Test
    public void testCreateSyncNode() throws KeeperException, InterruptedException {
        byte[] bytes = "创建zookeeper!".getBytes();
        String node1 = zooKeeper.create("/bxltest01", bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String node2 = zooKeeper.create("/bxltest02", bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        String node3 = zooKeeper.create("/bxltest03", bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        String node4 = zooKeeper.create("/bxltest04", bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("永久节点："+node1+"创建成功！！！");
        System.out.println("临时节点："+node2+"创建成功！！！");
        System.out.println("永久序列节点："+node3+"创建成功！！！");
        System.out.println("临时序列节点："+node4+"创建成功！！！");
        /**
         * 一旦关闭连接，临时节点就被删除了
         */
        Thread.sleep(30000); //睡30秒看看创建结果
    }

    /**
     * 异步创建节点
     */
    @Test
    public void testCreateAsyncNode(){
        byte[] bytes = "异步创建zookeeper!".getBytes();
        zooKeeper.create("/bxltestnode01",bytes,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,new IStringCallback(), "I am context. ");
    }


    /**
     * zk.exists(path,wathc:boolean)  判断某个节点是否存在,第二个参数boolean watcher
     * zk.delete(path,version) 使用 -1 是跳过版本检查，如果再删除的时候，会检查本地版本和远程版本若相同则会删除，否则不删除。同时不支持递归的删除
     *
     * 注意：zk是不可以递归删除的，必须一级一级的删
     *
     */
    @Test
    public void testDeleteNode() throws KeeperException, InterruptedException {

        Stat exists = zooKeeper.exists("/bxltest01", false);
        System.out.println(exists);
        if (exists!=null){
            zooKeeper.delete("/bxltest01",-1);
        }
        System.out.println("节点删除成功！！！");
    }

    /**
     * 异步删除节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void testAsyncDeleteNode() throws KeeperException, InterruptedException {

        Stat exists = zooKeeper.exists("/bxltestnode01", false);
        System.out.println(exists);
        if (exists!=null){
            zooKeeper.delete("/bxltestnode01",-1,new IVoidCallback(), null);
        }
        System.out.println("节点删除成功！！！");
    }


    /**
     *  注意：
     *      在更新数据时，setData方法存在一个version参数，其用于指定节点的数据版本，表明本次更新操作是针对指定的数据版本进行的，
     *      但是，在getData方法中，并没有提供根据指定数据版本来获取数据的接口，那么，这里为何要指定数据更新版本呢，这里方便理解，
     *      可以等效于CAS（compare and swap），对于值V，每次更新之前都会比较其值是否是预期值A，只有符合预期，才会将V原子化地更新到新值B。
     *      Zookeeper的setData接口中的version参数可以对应预期值，表明是针对哪个数据版本进行更新，假如一个客户端试图进行更新操作，
     *      它会携带上次获取到的version值进行更新，而如果这段时间内，Zookeeper服务器上该节点的数据已经被其他客户端更新，
     *      那么其数据版本也会相应更新，而客户端携带的version将无法匹配，无法更新成功，因此可以有效地避免分布式更新的并发问题。
     *
     * 结果表明：
     *      由于携带的数据版本不正确，而无法成功更新节点。其中，setData中的version参数设置-1含义为客户端需要基于数据的最新版本进行更新操作。
     */
    @Test
    public void testUpdateNode() throws KeeperException, InterruptedException {
        String path = "/bxltest01/demo02";
        Stat stat = zooKeeper.setData(path, "111111".getBytes(), -1);
        System.out.println("czxID: " + stat.getCzxid() + ", mzxID: " + stat.getMzxid() + ", version: " + stat.getVersion());
        Stat stat2 = zooKeeper.setData(path, "111111".getBytes(), stat.getVersion());
        System.out.println("czxID: " + stat2.getCzxid() + ", mzxID: " + stat2.getMzxid() + ", version: " + stat2.getVersion());
        try{
            //基于老版本数据更新会报错，因为老版本的数据已经被修改过了
            //zooKeeper.setData(path, "111111".getBytes(), stat.getVersion());
            //改为如下则不会报错
            zooKeeper.setData(path, "111111".getBytes(), -1);
        }catch (KeeperException e){
            System.out.println("进入异常！！！");
            System.out.println("Error: " + e.code() + "," + e.getMessage());
        }
        //Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 异步更新节点数据
     *
     * rc（ResultCode）为0，表明成功更新节点数据。
     */
    @Test
    public void testAsyncUpdateNode(){
        String path = "/bxltest01/demo02";
        zooKeeper.setData(path, "222222".getBytes(), -1, new IStatCallback(), null);
    }


    /**
     * 查询children node
     *
     * 注意：
     *      Watcher通知是一次性的，即一旦触发一次通知后，该Watcher就失效了，因此客户端需要反复注册Watcher，
     *      即程序中在process里面又注册了Watcher，否则，将无法获取c3节点的创建而导致子节点变化的事件。
     */
    @Test
    public void testGetChildrenNode() throws KeeperException, InterruptedException {
        String path = "/bxltest01";
        List<String> childrenList = zooKeeper.getChildren(path, true); //这里开启了watch事件
        System.out.println("子节点："+childrenList);
        //创建临时节点在这里触发修watch事件
        zooKeeper.create("/bxltest01/demo10", "testtmp".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Thread.sleep(3000);
        zooKeeper.create("/bxltest01/demo11", "testtmp".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Thread.sleep(3000);
    }

    /**
     * 异步查询children node
     *  结果表示通过异步的方式可以获取子节点信息。
     */
    @Test
    public void testGetAsyncChildrenNode() throws InterruptedException, KeeperException {
        String path = "/bxltest01";
        zooKeeper.getChildren(path, true, new IChildren2Callback(), null);
        //创建临时节点在这里触发修watch事件
        zooKeeper.create("/bxltest01/demo10", "testtmp".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Thread.sleep(3000);
        zooKeeper.create("/bxltest01/demo11", "testtmp".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Thread.sleep(3000);
    }

    /**
     * 查询节点的数据
     */
    @Test
    public void testGetNodeData() throws KeeperException, InterruptedException {
        String path = "/bxltest01/demo02";
        System.out.println("the data of znode " + path + " is : " + new String(zooKeeper.getData(path, true, stat)));
        System.out.println("czxID: " + stat.getCzxid() + ", mzxID: " + stat.getMzxid() + ", version: " + stat.getVersion());
        //改变节点数据,用来触发watch事件
        zooKeeper.setData(path, "123".getBytes(), -1);
        zooKeeper.setData(path, "456".getBytes(), -1);
    }

    /**
     * 异步查询节点的数据
     * 结果表明采用异步方式同样可方便获取节点的数据。
     */
    @Test
    public void testGetAsyncNodeData() throws KeeperException, InterruptedException {
        String path = "/bxltest01/demo02";
        zooKeeper.getData(path, true, new IDataCallback(), null);
        //改变节点数据,用来触发watch事件
        zooKeeper.setData(path, "678".getBytes(), -1);
        zooKeeper.setData(path, "789".getBytes(), -1);

    }


    /**
     *  在调用接口时注册Watcher的话，还可以对节点是否存在进行监听，一旦节点被创建、被删除、数据更新，都会通知客户端
     */
    @Test
    public void testIsNotExists() throws KeeperException, InterruptedException {
        String path = "/testbxl20";
        zooKeeper.exists(path, true);

        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.setData(path, "123".getBytes(), -1);

        zooKeeper.create(path + "/c1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("success create znode: " + path + "/c1");

        zooKeeper.delete(path + "/c1", -1);
        zooKeeper.delete(path, -1);
    }

    /**
     * 结果表明当节点不存在时，其rc（ResultCode）为-101。
     */
    @Test
    public void testAsyncIsNotExists() throws KeeperException, InterruptedException {
        String path = "/testbxl20";
        zooKeeper.exists(path, true, new IIStatCallback(), null);

        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.setData(path, "123".getBytes(), -1);

        zooKeeper.create(path + "/c1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("success create znode: " + path + "/c1");

        zooKeeper.delete(path + "/c1", -1);
        zooKeeper.delete(path, -1);

    }



}


class IStringCallback implements AsyncCallback.StringCallback{

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        System.out.println("Create path result: [" + rc + ", " + path + ", " + ctx + ", real path name: " + name);
    }
}

class IVoidCallback implements AsyncCallback.VoidCallback{

    @Override
    public void processResult(int rc, String path, Object ctx) {
        System.out.println(rc + ", " + path + ", " + ctx);
    }
}

class IChildren2Callback implements  AsyncCallback.Children2Callback{

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        System.out.println("Get Children znode result: [response code: " + rc + ", param path: " + path + ", ctx: "
                + ctx + ", children list: " + children + ", stat: " + stat);
    }
}

class IDataCallback implements  AsyncCallback.DataCallback{

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        System.out.println("异步数据》》》》》》》》》》");
        System.out.println("rc: " + rc + ", path: " + path + ", data: " + new String(data));
        System.out.println("czxID: " + stat.getCzxid() + ", mzxID: " + stat.getMzxid() + ", version: " + stat.getVersion());
        System.out.println("异步数据《《《《《《《《《《");
    }
}

class IStatCallback implements AsyncCallback.StatCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        System.out.println("rc: " + rc + ", path: " + path + ", stat: " + stat);
    }
}

class IIStatCallback implements AsyncCallback.StatCallback {

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        System.out.println("rc: " + rc + ", path: " + path + ", stat: " + stat);
    }
}