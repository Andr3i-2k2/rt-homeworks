package ssatr.device;

import java.util.concurrent.BlockingQueue;

/**
 * TASK SPORADIC: tratează alarmele de temperatură.
 *
 * Evenimentele sosesc în coada alarmEvents (puse de taskul periodic).
 * Intervalul minim între sosiri este MIN_INTERARRIVAL_MS: între două eliberări
 * ale taskului (două alarme publicate) trebuie să treacă cel puțin atât.
 * Intervalul este impus de cod: evenimentele care sosesc mai des sunt
 * respinse, ca să nu fie inundat brokerul.
 */
public class SporadicAlarmTask implements Runnable {

    static final long MIN_INTERARRIVAL_MS = 5000;
    long lastTimeStamp;

    private final String deviceId;
    private final MqttConnection mqtt;
    private final BlockingQueue<Event> alarmEvents;



    public SporadicAlarmTask(String deviceId, MqttConnection mqtt, BlockingQueue<Event> alarmEvents) {
        this.deviceId = deviceId;
        this.mqtt = mqtt;
        this.alarmEvents = alarmEvents;
        lastTimeStamp = System.currentTimeMillis();;
    }

    @Override
    public void run() {
        while (true) {
            try{
                Event event =alarmEvents.take();

                if(event.time-lastTimeStamp<MIN_INTERARRIVAL_MS){
                    System.out.println(
                            "Alarma respinsa pentru " + deviceId
                                    + ": interval prea mic."
                    );
                }else {
                    String message = String.format(
                            java.util.Locale.US,
                            "%s;%d;%.2f;%.2f",
                            deviceId,
                            event.time,
                            event.temperature,
                            event.threshold);

                    mqtt.publish(Config.alarmTopic(deviceId), message);

                    lastTimeStamp = event.time;

                    System.out.println("Alarma publicata: " + message);

                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        // TODO 2: implementați taskul sporadic:
        //   - așteptați un eveniment: alarmEvents.take()
        //   - dacă de la ultima eliberare au trecut mai puțin de
        //     MIN_INTERARRIVAL_MS, respingeți evenimentul (afișați un mesaj)
        //   - altfel publicați pe Config.alarmTopic(deviceId) mesajul
        //     "deviceId;timestamp;temperatura;prag"
        //     (pragul curent poate fi transmis împreună cu temperatura sau
        //      citit din PeriodicTelemetryTask; alegeți și justificați în raport)
        //
        // Vezi exemplul exemple/01-taskuri/.../SporadicTaskExample.java
    }
    }
}
