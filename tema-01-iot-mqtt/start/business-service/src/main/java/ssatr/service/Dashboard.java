
package ssatr.service;

import java.util.HashMap;
import java.util.Map;

public class Dashboard {

    private static class DeviceState {
        double temperature;
        long lastTimestamp;
        int alarms;
        String lastMessage;

        DeviceState() {
            temperature = 0.0;
            lastTimestamp = 0;
            alarms = 0;
            lastMessage = "-";
        }
    }

    private final Map<String, DeviceState> devices = new HashMap<>();


    public synchronized void updateTelemetry(
            String deviceId, long timestamp, double temperature) {

        DeviceState state = devices.computeIfAbsent(
                deviceId, k -> new DeviceState());

        state.temperature = temperature;
        state.lastTimestamp = timestamp;
        state.lastMessage = "Telemetry";
    }


    public synchronized void updateAlarm(
            String deviceId, long timestamp, double temperature) {

        DeviceState state = devices.computeIfAbsent(
                deviceId, k -> new DeviceState());

        state.temperature = temperature;
        state.lastTimestamp = timestamp;
        state.alarms++;
        state.lastMessage = "Alarm";
    }


    public synchronized void updateStatus(
            String deviceId, long timestamp, String message) {

        DeviceState state = devices.computeIfAbsent(
                deviceId, k -> new DeviceState());

        state.lastTimestamp = timestamp;
        state.lastMessage = message;
    }


    public synchronized void print() {

        System.out.printf("%-12s %-10s %-20s %-10s%n",
                "DEVICE", "TEMP", "ULTIMUL MESAJ", "ALARME");

        for (Map.Entry<String, DeviceState> entry : devices.entrySet()) {

            DeviceState state = entry.getValue();

            long secondsAgo =
                    (System.currentTimeMillis() - state.lastTimestamp) / 1000;

            System.out.printf("%-12s %-10.2f acum %d s %-10d%n",
                    entry.getKey(),
                    state.temperature,
                    secondsAgo,
                    state.alarms);
        }
    }
}
