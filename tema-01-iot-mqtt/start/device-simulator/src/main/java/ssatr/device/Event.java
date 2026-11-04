package ssatr.device;

public class Event {
    final int id;
    final long time;
    final double temperature;
    final double threshold;

    Event(int id, long time, double temperature, double threshold) {
        this.id = id;
        this.time = time;
        this.temperature = temperature;
        this.threshold = threshold;
    }
}
