package ssatr.device;

import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;

/**
 * TASK PERIODIC, cu perioada T = periodMs: fiecare job citește senzorul și
 * publică valoarea pe topic-ul de telemetrie.
 *
 * Dacă temperatura depășește pragul, taskul NU tratează el alarma, ci doar
 * semnalează un eveniment în coada alarmEvents. Alarma este tratată de
 * taskul sporadic.
 */
public class PeriodicTelemetryTask implements Runnable {

    private final String deviceId;
    private final TemperatureSensor sensor;
    private final MqttConnection mqtt;
    private final BlockingQueue<Event> alarmEvents;
    private int events;

    // volatile: sunt citite aici, dar modificate de taskul aperiodic (alt thread)
    private volatile long periodMs = 2000;
    private volatile double threshold = 28.0;

    public PeriodicTelemetryTask(String deviceId, TemperatureSensor sensor, MqttConnection mqtt,
                                 BlockingQueue<Event> alarmEvents) {
        this.deviceId = deviceId;
        this.sensor = sensor;
        this.mqtt = mqtt;
        this.alarmEvents = alarmEvents;
        events = 0;
    }

    @Override
    public void run() {
        long next = System.nanoTime();
        while (true) {



            double temp =this.sensor.read();
            long timestamp = System.currentTimeMillis();

            SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
            String date = sdf.format(new Date(timestamp));


            String message = String.format(
                    java.util.Locale.US,
                    "%s;%s;%.2f",
                    deviceId,
                    date.toString(),
                     temp
            );

            mqtt.publish(Config.telemetryTopic(deviceId),message);

            if (temp > threshold) {
                events++;
                Event e=new Event(events,timestamp ,temp, threshold );

                try {
                    alarmEvents.put(e);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }

            next += periodMs*1_000_000L;

            long remainingNs = next - System.nanoTime();

            if (remainingNs > 0) {
                try {
                    long sleepMs = remainingNs / 1_000_000L;
                    int sleepNs = (int) (remainingNs % 1_000_000L);

                    Thread.sleep(sleepMs, sleepNs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }


        }
        // TODO 1: implementați bucla taskului periodic. Fiecare job:
        //   - citiți temperatura: sensor.read()
        //   - publicați pe Config.telemetryTopic(deviceId) mesajul "deviceId;timestamp;temperatura"
        //     (timestamp = System.currentTimeMillis(); temperatura cu punct zecimal,
        //      de ex. String.format(Locale.US, "%.2f", t), ca să poată fi citită
        //      cu Double.parseDouble în business-service)
        //   - dacă temperatura > threshold, puneți valoarea în coada alarmEvents
        //
        // Folosiți așteptare absolută, nu relativă: Thread.sleep(periodMs) după
        // fiecare job duce la derivă (perioada reală devine T + C + întârzieri).
        // Calculați eliberarea următorului job pe grilă (next = next + periodMs)
        // și așteptați doar cât a mai rămas până la ea. Pentru grilă folosiți
        // un timp monoton: System.nanoTime().
        //
        // periodMs se poate schimba în timpul rulării (comanda SET_INTERVAL),
        // deci citiți-l la fiecare job.
    }

    public void setPeriodMs(long periodMs) {
        this.periodMs = periodMs;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public long getPeriodMs() {
        return periodMs;
    }

    public double getThreshold() {
        return threshold;
    }
}