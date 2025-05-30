package raft.entity;
public class Alamat {
    private String host;
    private int port ;

    public Alamat(){
        this.host = null;
        this.port = 0;
    }

    public Alamat(String host, int port){
        this.host = host;
        this.port = port;
    }

    public String getHost(){
        return this.host;
    }

    public void setHost(String host){
        this.host = host;
    }

    public int getPort(){
        return this.port;
    }

    public void setPort(int port){
        this.port = port;
    }


        @Override
    public String toString() {
        return host + ":" + port;
    }

        @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Alamat that = (Alamat) obj;
        return this.port == that.port && this.host.equals(that.host);
    }

        @Override
    public int hashCode() {
        return java.util.Objects.hash(host, port);
}

}
