import java.util.UUID;

public class TerminateMsg extends Msg {

    public TerminateMsg(UUID uuid) {
        super.action=Actions.TERMINATE.ordinal();
        super.uuid=uuid;
    }
}
