public class Address {
    private String host;
    private int port ;

    

    public Address(){
        this.host = null;
        this.port = 0;
    }

    public Address(String host, int port){
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

        @Override
    public String toString() {
        return host + ":" + port;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Address address = (Address) obj;
        return port == address.port && host.equals(address.host);
    }
    
    @Override
    public int hashCode() {
        return host.hashCode() * 31 + port;
    }
}
