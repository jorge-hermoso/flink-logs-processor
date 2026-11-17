package cloud.hermoso.flink_logs_processor;

import cloud.hermoso.flink_logs_processor.config.StationProfile;
import cloud.hermoso.flink_logs_processor.config.StationProfiles;

import java.util.List;

/** Station profiles shared by the tests. */
public final class TestProfiles {
    private TestProfiles() {
    }

    public static StationProfiles profiles() {
        return new StationProfiles()
                .add(new StationProfile("KILN", List.of("o2", "zone_temp"), List.of("o2", "zone temp reading")))
                .add(new StationProfile("PRESS", List.of("peak_force"), List.of("peak_force")))
                .add(new StationProfile("PACKER", List.of("rate", "rejects"), List.of("rate", "rejects")));
    }
}
