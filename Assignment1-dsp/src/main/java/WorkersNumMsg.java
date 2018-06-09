import java.util.UUID;

public class WorkersNumMsg extends Msg {
    int num;

    public WorkersNumMsg(int num, UUID uuid) {
        super.action=Actions.WORKERNUM.ordinal();
        super.uuid=uuid;
        this.num = num;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
