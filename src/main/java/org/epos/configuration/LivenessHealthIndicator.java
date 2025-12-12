package org.epos.configuration;

import dao.EposDataModelDAO;
import model.Person;
import org.epos.router_framework.RpcRouter;
import org.epos.router_framework.exception.RoutingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class LivenessHealthIndicator implements HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(LivenessHealthIndicator.class);

    private final RpcRouter router;

    public LivenessHealthIndicator(RpcRouter router) {
        this.router = router;
    }

    @Override
    public Health health() {

        try {
            if(!router.doHealthCheck()){
                return Health.down().withDetail("No Router Connection", 1).build();
            }
        }catch(Exception e){
            return Health.down().withDetail("No Router Connection", 1).build();
        }
        try {
            EposDataModelDAO.getInstance().getAllFromDB(Person.class);
        }catch(Exception e){
            return Health.down().withDetail("No Database Connection", 1).build();
        }
        return Health.up().build();
    }
}
