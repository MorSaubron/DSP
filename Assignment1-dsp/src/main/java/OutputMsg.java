import java.util.ArrayList;
import java.util.UUID;

public class OutputMsg extends Msg {
    String url;
    String fileName;

    public OutputMsg(String url, String fileName, UUID uuid) {
        super.action=Actions.OUTPUT.ordinal();
        super.uuid=uuid;
        this.url = url;
        this.fileName=fileName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
