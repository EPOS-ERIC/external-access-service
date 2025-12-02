package org.epos.core;

import dao.EposDataModelDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

@Component
public class ScheduledRuntimes {

	private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledRuntimes.class);

	@PostConstruct
	public void onStartup() {
		LOGGER.info("[External Service Startup] -----------------------------------------------");
		LOGGER.info("[StartUp Task - Resources] Updating facets information");
		connectionsUpdater();
		LOGGER.info("[StartUp Task - Resources] Done");
		LOGGER.info("[External Service Startup Completed] -----------------------------------------------");
	}

	@Scheduled(fixedRate = 60000, initialDelay = 0)
	@Async
	public void connectionsUpdater() {
		LOGGER.info("[Scheduled Task - Resources] Updating resources information");
		EposDataModelDAO.getInstance().clearAllCaches();
        LOGGER.info("[Scheduled Task - Resources] Resources successfully updated");
	}

}
