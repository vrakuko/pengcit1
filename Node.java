import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



// import java.util.ArrayList ; 
class Addr { 
    private String host;
    private int port ;

    public Addr(String host, int port){
        this.host = host;
        this.port = port;
    }

    public String getHost(){
        return this.host;
    }

    public void setString(String host){
        this.host = host;
    }

    public int getPort(){
        return this.port;
    }

    public void setPort(int port){
        this.port = port;
    }

}

class Node {

    private static final double HEARTBEAT_INTERVAL = 200, ELECTION_TIMEOUT_MIN  = 100, ELECTION_TIMEOUT_MAX= 200, RPC_TIMEOUT = 0.5;
    private Addr addr ; 
    private NodeType nodeType ; 
    private Map<String, String> log ;
    private int election_term ;
    private List<Addr> clust_adr_list ;
    private Addr clust_leader_adr ; 

    private Object app;
    
    enum NodeType {LEADER, FOLLOWER, CANDIDATE}

    public Node(Addr addr,  Addr clust_leader_adr, Object application, Addr contact_adr){
        this.nodeType = Node.NodeType.FOLLOWER;
        this.addr = addr;
        this.log = new HashMap<>();
        this.election_term = 0;
        this.clust_adr_list = new ArrayList<>();
        this.clust_leader_adr = clust_leader_adr;
        if (contact_adr == null){
            this.clust_adr_list.add(this.addr);
            this.initializeAsLeader();
        }else{
            this.joinMembership(contact_adr);
        }
    }

    // public void printLog(String txt){
    //     System.out.println("["+this.addr.getHost()+"] ["+LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))+"] "+txt);
    // }

    public void setLog(String k, String v){
        // Map<String,String> entri = new HashMap<>();
        // entri.put(k,v);
        this.log.put(k,v);
    }

    public Map<String, String> getLog(){
        return this.log;
    }


    public void initializeAsLeader(){

    }

    public  void joinMembership(Addr addr){

    }


    public void  leaderHeartbeat(){

    }


    public void ping(){
        boolean connected = true; // will be implemmented later
        if(connected==true){
            System.out.println("PONG!");
        }
    }
    
    public String get(String k){
        return this.log.get(k);
    }

    public void set(String k, String v){
        if (this.log.containsKey(k)){
            this.log.replace(k, this.log.get(k), v);
        }else{
            this.log.put(k,v);
        }
    }

    public int strln(String k){
        return k.length() ;
    }

    public String del(String k){
        String n = this.log.get(k);
        if(n == null){
            return "";
        }
        this.log.remove(k, n);
        return n;
    }

    public String append(String k, String v){
        if (!this.log.containsKey(k)){
            this.log.put(k," ");
            
        }
        String x= this.log.get(k).concat(v);
        return x;
    }

}

