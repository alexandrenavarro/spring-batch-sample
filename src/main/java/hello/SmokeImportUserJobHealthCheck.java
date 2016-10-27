package hello;

import org.springframework.batch.core.BatchStatus;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

/**
 * Created by anavarro on 27/10/16.
 */
@ManagedResource
public class SmokeImportUserJobHealthCheck implements HealthIndicator {


    private BatchStatus batchStatus = BatchStatus.UNKNOWN;


    @Override
    public Health health() {
        if (BatchStatus.COMPLETED != batchStatus) {
            return Health.down().withDetail("Error Code", batchStatus.getBatchStatus().name()).build();
        }
        return Health.up().build();
    }

    @ManagedOperation
    public void setBatchStatusString(String batchStatus) {
        setBatchStatus(BatchStatus.valueOf(batchStatus));
    }


    public void setBatchStatus(BatchStatus batchStatus) {
        this.batchStatus = batchStatus;
    }
}
