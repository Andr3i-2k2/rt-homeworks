package ssatr.service;

import java.util.concurrent.BlockingQueue;

/**
 * Preia mesajele de un anumit tip ("telemetry", "alarm" sau "status") din coada
 * inbox, le salvează și actualizează dashboard-ul.
 *
 * Formatul mesajelor (câmpuri separate prin ";"):
 *   telemetry:  deviceId;timestamp;temperatura
 *   alarm:      deviceId;timestamp;temperatura;prag
 *   status:     deviceId;timestamp;text
 */
public class MessageProcessor implements Runnable {

    private final String kind;
    private final BlockingQueue<String> inbox;
    private final CsvStorage storage;
    private final Dashboard dashboard;

    public MessageProcessor(String kind, BlockingQueue<String> inbox, CsvStorage storage, Dashboard dashboard) {
        this.kind = kind;
        this.inbox = inbox;
        this.storage = storage;
        this.dashboard = dashboard;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {

                String mesaj = inbox.take();

                try {

                    String[] parts = mesaj.split(";", -1);


                    switch (kind) {
                        case "telemetry":
                            if (parts.length != 3) {
                                throw new IllegalArgumentException(
                                        "Format telemetry invalid");
                            }

                            dashboard.updateTelemetry(
                                    parts[0],
                                    Long.parseLong(parts[1]),
                                    Double.parseDouble(parts[2])
                            );
                            break;

                        case "alarm":
                            if (parts.length != 4) {
                                throw new IllegalArgumentException(
                                        "Format alarm invalid");
                            }

                            dashboard.updateAlarm(
                                    parts[0],
                                    Long.parseLong(parts[1]),
                                    Double.parseDouble(parts[2])
                            );

                            System.out.println("[ALARM] " + mesaj);
                            break;

                        case "status":
                            if (parts.length != 3) {
                                throw new IllegalArgumentException(
                                        "Format status invalid");
                            }

                            dashboard.updateStatus(
                                    parts[0],
                                    Long.parseLong(parts[1]),
                                    parts[2]
                            );

                            System.out.println("[STATUS] " + mesaj);
                            break;

                        default:
                            throw new IllegalArgumentException(
                                    "Tip mesaj necunoscut: " + kind);
                    }


                    storage.append(kind, mesaj);

                } catch (Exception ex) {
                    System.err.println(
                            "[ERROR] Mesaj invalid (" + kind + "): "
                                    + mesaj + " | " + ex.getMessage()
                    );
                }

            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
