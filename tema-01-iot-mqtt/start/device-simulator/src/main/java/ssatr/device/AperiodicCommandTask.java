package ssatr.device;

import java.util.concurrent.BlockingQueue;

/**
 * TASK APERIODIC: execută comenzile primite de la business service.
 *
 * Comenzile sosesc oricând, pe MQTT, fără un interval minim între ele, și sunt
 * puse în coada commands. Fiecare comandă eliberează un job al taskului.
 * Comenzi care trebuie suportate:
 *
 *   SET_INTERVAL <ms>       schimbă perioada T a taskului periodic
 *   SET_THRESHOLD <valoare> schimbă pragul de alarmă
 *   RESET                   readuce senzorul la valoarea inițială
 *   STATUS                  raportează perioada și pragul curente
 *
 * După executarea fiecărei comenzi, device-ul publică pe Config.statusTopic(deviceId)
 * o confirmare: "deviceId;timestamp;text", de exemplu
 * "device-1;1727600000000;OK SET_INTERVAL 1000".
 */
public class AperiodicCommandTask implements Runnable {

    private final String deviceId;
    private final TemperatureSensor sensor;
    private final MqttConnection mqtt;
    private final BlockingQueue<String> commands;
    private final PeriodicTelemetryTask periodic;

    public AperiodicCommandTask(String deviceId, TemperatureSensor sensor, MqttConnection mqtt,
                                BlockingQueue<String> commands, PeriodicTelemetryTask periodic) {
        this.deviceId = deviceId;
        this.sensor = sensor;
        this.mqtt = mqtt;
        this.commands = commands;
        this.periodic = periodic;
    }

    @Override
    public void run() {
        try {
            while (true) {
                String c = commands.take();
                System.out.println("comanda"+c);

                String[] parts = c.split("\\s+", 2);
                String command = parts[0].toUpperCase();
                String param = parts.length > 1 ? parts[1] : "";

                String response;

                try {
                    switch (command) {
                        case "SET_INTERVAL":
                            long period = Long.parseLong(param);

                            if (period <= 0) {
                                throw new IllegalArgumentException(
                                        "intervalul trebuie sa fie pozitiv"
                                );
                            }

                            periodic.setPeriodMs(period);
                            response = "OK SET_INTERVAL " + period;
                            break;

                        case "SET_THRESHOLD":
                            double threshold = Double.parseDouble(param);

                            if (!Double.isFinite(threshold)) {
                                throw new IllegalArgumentException(
                                        "pragul trebuie sa fie un numar finit"
                                );
                            }

                            periodic.setThreshold(threshold);
                            response = String.format(
                                    java.util.Locale.US,
                                    "OK SET_THRESHOLD %.2f",
                                    threshold
                            );
                            break;

                        case "RESET":
                            if (!param.isEmpty()) {
                                throw new IllegalArgumentException(
                                        "RESET nu accepta parametri"
                                );
                            }

                            sensor.reset();
                            response = "OK RESET";
                            break;

                        case "STATUS":
                            if (!param.isEmpty()) {
                                throw new IllegalArgumentException(
                                        "STATUS nu accepta parametri"
                                );
                            }

                            response = String.format(
                                    java.util.Locale.US,
                                    "OK STATUS interval=%d threshold=%.2f",
                                    periodic.getPeriodMs(),
                                    periodic.getThreshold()
                            );
                            break;

                        case "GET_THRESHOLD":
                            if (!param.isEmpty()) {
                                throw new IllegalArgumentException(
                                  "GET_INTERVAL nu accepta parametri"
                                );
                            }
                            response =String.format(
                                    java.util.Locale.US,
                                    "OK STATUS threshold=%.2f",
                                    periodic.getThreshold()

                            );
                            break;

                        case "GET_INTERVAL":
                            if (!param.isEmpty()) {
                                throw new IllegalArgumentException(
                                        "GET_INTERVAL nu accepta parametri"
                                );
                            }
                            response =String.format(
                                    java.util.Locale.US,
                                    "OK STATUS interval=%d .2f",
                                    periodic.getPeriodMs()

                            );
                            break;

                        default:
                            throw new IllegalArgumentException(
                                    "comanda necunoscuta: " + command
                            );
                    }

                } catch (IllegalArgumentException ex) {
                    response = "ERROR " + ex.getMessage();
                }

                // Publică confirmarea sau eroarea
                String statusMessage = String.format(
                        java.util.Locale.US,
                        "%s;%d;%s",
                        deviceId,
                        System.currentTimeMillis(),
                        response
                );

                mqtt.publish(
                        Config.statusTopic(deviceId),
                        statusMessage
                );

                System.out.println("Status: " + statusMessage);


            }
        }catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        // TODO 3: implementați taskul aperiodic:
        //   - preluați comenzile din coadă: commands.take()
        //   - separați numele comenzii de parametru: command.split(" ")
        //   - executați comanda (periodic.setPeriodMs(...), sensor.reset() etc.)
        //   - publicați confirmarea pe Config.statusTopic(deviceId)
        //   - o comandă necunoscută sau un parametru invalid nu trebuie să
        //     oprească taskul: publicați "ERROR <motiv>"
        //
        // Vezi exemplul exemple/01-taskuri/.../AperiodicTaskExample.java
    }
}
