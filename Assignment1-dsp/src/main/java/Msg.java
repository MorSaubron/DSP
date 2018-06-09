import java.util.UUID;

public class Msg {
   protected int action;
   protected UUID uuid;
    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }
}
