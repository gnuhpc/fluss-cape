package org.gnuhpc.fluss.cape.pg.protocol;

import io.netty.buffer.ByteBuf;
import org.gnuhpc.fluss.cape.pg.protocol.backend.PgBackendMessage;

public final class PgBackendKeyDataPacket implements PgBackendMessage {

    private final int processId;
    private final int secretKey;

    public PgBackendKeyDataPacket(int processId, int secretKey) {
        this.processId = processId;
        this.secretKey = secretKey;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeByte('K');
        out.writeInt(12);
        out.writeInt(processId);
        out.writeInt(secretKey);
    }
}
