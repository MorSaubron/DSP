import java.util.UUID;

public class UrlMsg extends Msg {
    String bucketName;
    String key;
    String URL;


    public UrlMsg(String bucketName, String key, String URL, UUID uuid) {
        super.action = Actions.URL.ordinal();
        super.uuid=uuid;
        this.bucketName = bucketName;
        this.key = key;
        this.URL = URL;

    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getURL() {
        return URL;
    }

    public void setURL(String URL) {
        this.URL = URL;
    }

    @Override
    public String toString(){
        String s=bucketName+" "+key+" "+URL+" "+uuid;
        return s;
    }

}

